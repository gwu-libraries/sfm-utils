from __future__ import absolute_import
import getpass
import os
import stat
from supervisor import xmlrpc
import time
import xmlrpclib
import logging
import json
from sfmutils.utils import safe_string

log = logging.getLogger(__name__)


class HarvestSupervisor():
    def __init__(self, script, mq_host, mq_username, mq_password,
                 process_owner=None, python_executable="python", log_path="/var/log/sfm",
                 conf_path="/etc/supervisor/conf.d", internal_ip="127.0.0.1", socket_file="/var/run/supervisor.sock"):
        self.conf_path = conf_path
        self.process_owner = process_owner or getpass.getuser()
        self.python_executable = python_executable
        self.script = script
        self.mq_host = mq_host
        self.mq_username = mq_username
        self.mq_password = mq_password
        self.log_path = log_path
        self.internal_ip = internal_ip
        self.socket_file = socket_file

        if not os.path.exists(self.conf_path):
            log.debug("Creating %s", self.conf_path)
            os.makedirs(self.conf_path)

        if not os.path.exists(self.log_path):
            log.debug("Creating %s", self.log_path)
            os.makedirs(self.log_path)

    def start(self, harvest_start_message, routing_key):
        log.info("Starting %s", harvest_start_message)
        harvest_id = harvest_start_message["id"]

        #Stop existing
        self.stop(harvest_id)

        #Write seed file
        with open(self._get_seed_filepath(harvest_id), 'w') as f:
            json.dump(harvest_start_message, f)

        #Create conf file
        self._create_conf_file(harvest_id, routing_key, )

        time.sleep(1)
        self._reload_config()
        self._add_process_group(harvest_id)

    def stop(self, harvest_id):
        log.info("Stopping %s", harvest_id)

        #Remove process group
        self._remove_process_group(harvest_id)

        #Delete conf file
        conf_filepath = self._get_conf_filepath(harvest_id)
        if os.path.exists(conf_filepath):
            log.debug("Deleting conf %s", conf_filepath)
            os.remove(conf_filepath)

        #Delete seed file
        seed_filepath = self._get_seed_filepath(harvest_id)
        if os.path.exists(seed_filepath):
            log.debug("Deleting seed %s", seed_filepath)
            os.remove(seed_filepath)

    def _create_conf_file(self, harvest_id, routing_key):
        contents = """[program:{process_group}]
command={python_executable} {script} seed {seed_filepath} --streaming --host {mq_host} --username {mq_username} --password {mq_password} --routing-key {routing_key}
user={user}
autostart=true
autorestart=true
stderr_logfile={log_path}/{safe_harvest_id}.err.log
stdout_logfile={log_path}/{safe_harvest_id}.out.log
""".format(process_group=self._get_process_group(harvest_id),
           safe_harvest_id=safe_string(harvest_id),
           python_executable=self.python_executable,
           script=self.script,
           seed_filepath=self._get_seed_filepath(harvest_id),
           mq_host=self.mq_host,
           mq_username=self.mq_username,
           mq_password=self.mq_password,
           routing_key=routing_key,
           user=self.process_owner,
           log_path=self.log_path)

        # Write the file
        conf_filepath = self._get_conf_filepath(harvest_id)
        log.debug("Writing conf to %s: %s", conf_filepath, contents)
        with open(conf_filepath, "wb") as f:
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
        return xmlrpclib.ServerProxy(
            "http://{}".format(self.internal_ip),
            transport=xmlrpc.SupervisorTransport(
                None, None, "unix://{}".format(self.socket_file)))

    @staticmethod
    def _get_process_group(harvest_id):
        return safe_string(harvest_id)

    def _add_process_group(self, harvest_id):
        process_group = self._get_process_group(harvest_id)
        log.debug("Adding process group %s", process_group)
        try:
            self._get_supervisor_proxy().supervisor.addProcessGroup(process_group)
        except xmlrpclib.Fault as e:
            if e.faultCode != xmlrpc.Faults.ALREADY_ADDED:
                raise
            # else ignore - it's already added
            # but everything else, we want to raise

    def _remove_process_group(self, harvest_id):
        proxy = self._get_supervisor_proxy()
        process_group = self._get_process_group(harvest_id)
        log.debug("Removing process group %s", process_group)
        try:
            proxy.supervisor.stopProcess(process_group, True)
        except xmlrpclib.Fault as e:
            if e.faultCode == xmlrpc.Faults.BAD_NAME:
                # process isn't known, so there's nothing to stop
                # or remove
                return
            elif e.faultCode != xmlrpc.Faults.NOT_RUNNING:
                raise
            # else ignore and proceed - it's already stopped
            # nothing to stop
        time.sleep(1)
        try:
            proxy.supervisor.removeProcessGroup(process_group)
        except xmlrpclib.Fault as e:
            if e.faultCode != xmlrpc.Faults.BAD_NAME:
                raise
            # else do nothing - no such known process, so there's
            # nothing to remove

    def _reload_config(self):
        log.debug("Reloading config")
        self._get_supervisor_proxy().supervisor.reloadConfig()
