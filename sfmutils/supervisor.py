import getpass
import os
import stat
from supervisor.xmlrpc import SupervisorTransport, Faults
import time
import xmlrpc
import logging
import json
from sfmutils.utils import safe_string

log = logging.getLogger(__name__)


class HarvestSupervisor:
    def __init__(self, script, mq_host, mq_username, mq_password, working_path,
                 process_owner=None, python_executable="python", log_path=None,
                 conf_path="/etc/supervisor/conf.d", internal_ip="127.0.0.1", socket_file="/var/run/supervisor.sock",
                 debug=False):
        self.conf_path = conf_path
        self.process_owner = process_owner or getpass.getuser()
        self.python_executable = python_executable
        self.script = script
        self.mq_host = mq_host
        self.mq_username = mq_username
        self.mq_password = mq_password
        self.working_path = working_path
        self.log_path = log_path or os.path.join(self.working_path, "log")
        self.internal_ip = internal_ip
        self.socket_file = socket_file
        if debug:
            log.info("Don't forget that log files are in %s", self.log_path)
        self.debug = debug

        if not os.path.exists(self.conf_path):
            log.debug("Creating %s", self.conf_path)
            os.makedirs(self.conf_path)

        if not os.path.exists(self.log_path):
            log.debug("Creating %s", self.log_path)
            os.makedirs(self.log_path)

    def start(self, harvest_start_message, routing_key, debug=False, debug_warcprox=False, tries=3):
        log.info("Starting %s: %s", routing_key, harvest_start_message)
        harvest_id = harvest_start_message["id"]

        # Remove existing
        self.remove(harvest_id)

        # Write seed file
        with open(self._get_seed_filepath(harvest_id), 'w') as f:
            json.dump({
                "routing_key": routing_key,
                "message": harvest_start_message
            }, f)

        # Create conf file
        self._create_conf_file(harvest_id, debug, debug_warcprox, tries)

        time.sleep(1)
        self._reload_config()
        self._add_process_group(harvest_id)

    def remove(self, harvest_id):
        log.info("Removing %s", harvest_id)

        # Remove process group
        self._remove_process_group(harvest_id)

        # Delete conf file
        conf_filepath = self._get_conf_filepath(harvest_id)
        if os.path.exists(conf_filepath):
            log.debug("Deleting conf %s", conf_filepath)
            os.remove(conf_filepath)

        # Delete seed file
        seed_filepath = self._get_seed_filepath(harvest_id)
        if os.path.exists(seed_filepath):
            log.debug("Deleting seed %s", seed_filepath)
            os.remove(seed_filepath)

    def pause_all(self):
        log.info("Pausing all")
        # Sending USR1 to tell the harvester that SIGTERM should trigger a pause of the harvest, not an end.
        # For a pause, will attempt to cleanly exit without completing harvest.
        self._get_supervisor_proxy().supervisor.signalAllProcesses("USR1")
        self._get_supervisor_proxy().supervisor.stopAllProcesses()

    def _create_conf_file(self, harvest_id, debug, debug_warcprox, tries):
        # Note that giving a long time to shutdown.
        # Stream harvester may need to finish processing.
        contents = """[program:{process_group}]
command={python_executable} {script} --debug={debug} --debug-warcprox={debug_warcprox} seed {seed_filepath} {working_path} --streaming --host {mq_host} --username {mq_username} --password {mq_password} --tries {tries}
user={user}
autostart=true
autorestart=unexpected
exitcodes=0,1
stopwaitsecs=900
stderr_logfile={log_path}/{safe_harvest_id}.err.log
stdout_logfile={log_path}/{safe_harvest_id}.out.log
""".format(process_group=self._get_process_group(harvest_id),
           safe_harvest_id=safe_string(harvest_id),
           python_executable=self.python_executable,
           script=self.script,
           seed_filepath=self._get_seed_filepath(harvest_id),
           working_path=self.working_path,
           mq_host=self.mq_host,
           mq_username=self.mq_username,
           mq_password=self.mq_password,
           user=self.process_owner,
           log_path=self.log_path,
           debug=debug,
           debug_warcprox=debug_warcprox,
           tries=tries)

        # Write the file
        conf_filepath = self._get_conf_filepath(harvest_id)
        log.debug("Writing conf to %s: %s", conf_filepath, contents)
        with open(conf_filepath, "w") as f:
            f.write(contents)
        filestatus = os.stat(conf_filepath)
        # do a chmod +x, and add group write permissions
        os.chmod(conf_filepath, filestatus.st_mode | stat.S_IXUSR |
                 stat.S_IXGRP | stat.S_IXOTH | stat.S_IWGRP)

    def _get_conf_filepath(self, harvest_id):
        return "{}/{}.conf".format(self.conf_path, safe_string(harvest_id))

    def _get_seed_filepath(self, harvest_id):
        return "{}/{}.json".format(self.conf_path, safe_string(harvest_id))

    def _get_supervisor_proxy(self):
        return xmlrpc.client.ServerProxy(
            "http://{}".format(self.internal_ip),
            transport=SupervisorTransport(
                None, None, "unix://{}".format(self.socket_file)))

    @staticmethod
    def _get_process_group(harvest_id):
        return safe_string(harvest_id)

    def _add_process_group(self, harvest_id):
        process_group = self._get_process_group(harvest_id)
        log.debug("Adding process group %s", process_group)
        try:
            self._get_supervisor_proxy().supervisor.addProcessGroup(process_group)
        except xmlrpc.client.Fault as e:
            if e.faultCode != Faults.ALREADY_ADDED:
                raise
            # else ignore - it's already added
            # but everything else, we want to raise

    def _remove_process_group(self, harvest_id):
        proxy = self._get_supervisor_proxy()
        process_group = self._get_process_group(harvest_id)
        log.debug("Removing process group %s", process_group)
        try:
            proxy.supervisor.stopProcess(process_group, True)
        except xmlrpc.client.Fault as e:
            if e.faultCode == Faults.BAD_NAME:
                # process isn't known, so there's nothing to stop
                # or remove
                return
            elif e.faultCode != Faults.NOT_RUNNING:
                raise
            # else ignore and proceed - it's already stopped
            # nothing to stop
        time.sleep(1)
        try:
            proxy.supervisor.removeProcessGroup(process_group)
        except xmlrpc.client.Fault as e:
            if e.faultCode != Faults.BAD_NAME:
                raise
            # else do nothing - no such known process, so there's
            # nothing to remove

    def _reload_config(self):
        log.debug("Reloading config")
        self._get_supervisor_proxy().supervisor.reloadConfig()
