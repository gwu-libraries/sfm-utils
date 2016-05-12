import subprocess
import atexit
import logging
import sys
from time import sleep
import os
import socket
import tempfile
import shutil

log = logging.getLogger(__name__)


class SubProcess(object):
    """
    Track a subprocess from command-line.

    Add atexit callback to terminate it on shutdown.

    Borrowed from https://github.com/ikreymer/pywb-webrecorder/blob/master/pywb-webrecorder.py
    """
    def __init__(self, cl):
        """
        Launch subprocess
        """
        log.info("Executing %s", cl)
        args = cl.split(' ')
        self.name = args[0]
        self.proc = subprocess.Popen(args, stdout=sys.stdout)
        atexit.register(self.cleanup)

    def cleanup(self):
        """
        Terminate subprocess, wait for it to finish
        """
        try:
            log.info("Shutting down %s", self.name)
            if self.proc:
                log.debug("Terminating %s", self.name)
                self.proc.terminate()
            log.debug("Waiting for %s to terminate", self.name)
            self.proc.wait()
            log.debug("%s terminated", self.name)
        except Exception:
            try:
                log.debug("Killing %s", self.name)
                self.proc.kill()
                log.debug("Killed %s", self.name)
            except Exception:
                pass


class warced():
    """
    An entry/exit wrapper for warcprox.

    An instance of warcprox will be spawned on entry and terminated on exit.

    Also, the environment variables HTTP_PROXY, HTTPS_PROXY, REQUESTS_CA_BUNDLE
    are set. This will properly configure the requests library to use the proxy;
    other configuration may be necessary for other HTTP libraries.
    """
    def __init__(self, prefix, directory, compress=True, port=None, debug=False, interrupt=False):
        """
        :param prefix: prefix for the WARC filename.
        :param directory: directory into which to place the WARCS.
        :param compress: gzip compress the WARC. Default is true.
        :param port: Port on which to run the proxy. If not provided, an open
        port will be selected.
        :param debug: If True, runs warcprox with verbose option.
        :param interrupt: If True, interrupts request when warcprox receives SIGTERM.
        """
        self.directory = directory
        self.prefix = prefix
        self.port = port or self._pick_a_port()
        self.compress = compress
        self.warcprox = None
        self.interrupt = interrupt
        self.ca_dir = tempfile.mkdtemp()
        self.ca_bundle = os.path.join(self.ca_dir, "warcprox-ca.pem")
        self.debug = debug

    def __enter__(self):
        # Set environment variables that requests uses to configure proxy
        self._set_envs()

        self.warcprox = SubProcess(self._generate_commandline())
        # Wait for it to start up
        sleep(5)

        return self

    def _set_envs(self):
        os.environ["HTTP_PROXY"] = "localhost:{}".format(self.port)
        os.environ["HTTPS_PROXY"] = "localhost:{}".format(self.port)
        os.environ["REQUESTS_CA_BUNDLE"] = self.ca_bundle

    def _unset_envs(self):
        self._unset_env("HTTP_PROXY")
        self._unset_env("HTTPS_PROXY")
        self._unset_env("REQUESTS_CA_BUNDLE")

    @staticmethod
    def _unset_env(key):
        if key in os.environ:
            del os.environ[key]

    @staticmethod
    def _pick_a_port():
        port = 8000
        while True:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if s.connect_ex(('', port)):
                return port
            port += 1

    def _generate_commandline(self):
        cl = "warcprox -c {} --certs-dir {} --dedup-db-file /dev/null -d {} -n {} -p {}".format(self.ca_bundle,
                                                                                                self.ca_dir,
                                                                                                self.directory,
                                                                                                self.prefix,
                                                                                                self.port)
        if self.compress:
            cl += " -z"
        if self.debug:
            cl += " -v"
        if self.interrupt:
            cl += " -i"
        return cl

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.warcprox:
            self.warcprox.cleanup()
        self._unset_envs()
        if os.path.exists(self.ca_dir):
            shutil.rmtree(self.ca_dir)
