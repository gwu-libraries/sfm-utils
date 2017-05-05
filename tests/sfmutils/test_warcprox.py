from __future__ import absolute_import
from unittest import TestCase
from sfmutils.warcprox import warced, SubProcess
from subprocess import Popen
import os
import socket
import requests
import tempfile
import shutil
from time import sleep
from mock import patch, MagicMock
import sys


class WarcedTest(TestCase):
    def test_set_env(self):
        self.assertIsNone(os.environ.get("HTTP_PROXY"))
        self.assertIsNone(os.environ.get("HTTPS_PROXY"))
        self.assertIsNone(os.environ.get("REQUESTS_CA_BUNDLE"))
        w = warced(None, None, port=1234)
        w._set_envs()
        self.assertEqual("localhost:1234", os.environ["HTTP_PROXY"])
        self.assertEqual("localhost:1234", os.environ["HTTPS_PROXY"])

        w._unset_envs()
        self.assertIsNone(os.environ.get("HTTP_PROXY"))
        self.assertIsNone(os.environ.get("HTTPS_PROXY"))
        self.assertIsNone(os.environ.get("REQUESTS_CA_BUNDLE"))

    def test_pick_a_port(self):
        port = warced._pick_a_port()
        self.assertTrue(port >= 7000)
        # Make sure port is available
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.assertNotEqual(0, sock.connect_ex(('', port)))

    def test_generate_commandline(self):
        w = warced("test", "/test")
        self.assertEqual("warcprox -c {} --certs-dir {} --dedup-db-file /dev/null -d /test -n test -p {} -z".format(
            w.ca_bundle, w.ca_dir, w.port), w._generate_commandline())

        w = warced("test", "/test", compress=False, interrupt=True, rollover_time=60)
        self.assertEqual(
            "warcprox -c {} --certs-dir {} --dedup-db-file /dev/null -d /test -n test -p {} -i "
            "--rollover-time 60".format(w.ca_bundle, w.ca_dir, w.port), w._generate_commandline())

    def test_with(self):
        warc_dir = tempfile.mkdtemp()
        try:
            with warced("test", warc_dir):
                resp = requests.get("http://www.gwu.edu")
                self.assertEqual(200, resp.status_code)
            files = os.listdir(warc_dir)
            self.assertEqual(1, len(files))
            self.assertTrue(files[0].startswith("test"))
            self.assertTrue(files[0].endswith(".warc.gz"))
        finally:
            shutil.rmtree(warc_dir)


class SubprocessTest(TestCase):
    def test_process_end(self):
        subprocess = SubProcess("sleep 1")
        self.assertIsNotNone(subprocess.proc)
        sleep(2)
        subprocess.cleanup()
        self.assertIsNone(subprocess.proc)

    def test_terminate(self):
        subprocess = SubProcess("sleep 35")
        self.assertIsNotNone(subprocess.proc)
        subprocess.cleanup()
        self.assertIsNone(subprocess.proc)

    @patch("sfmutils.warcprox.Popen", autospec=True)
    def test_kill(self, mock_popen_cls):
        mock_popen = MagicMock(spec=Popen)
        mock_popen_cls.side_effect = [mock_popen]

        subprocess = SubProcess("foo", terminate_wait_secs=2)
        mock_popen_cls.assert_called_once_with(["foo"], stdout=sys.stdout)

        mock_popen.poll.return_value = None
        subprocess.cleanup()

        mock_popen.terminate.assert_called_once_with()
        mock_popen.kill.assert_called_once_with()
