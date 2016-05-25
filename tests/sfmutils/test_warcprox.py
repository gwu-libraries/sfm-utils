from __future__ import absolute_import
from unittest import TestCase
from sfmutils.warcprox import warced
import os
import socket
import requests
import tempfile
import shutil


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
        self.assertTrue(port >= 8000)
        # Make sure port is available
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.assertNotEqual(0, sock.connect_ex(('', port)))

    def test_generate_commandline(self):
        w = warced("test", "/test")
        self.assertEqual("warcprox -c {} --certs-dir {} --dedup-db-file /dev/null -d /test -n test -p {} -z".format(
                          w.ca_bundle, w.ca_dir, w.port), w._generate_commandline())

        w = warced("test", "/test", compress=False, interrupt=True)
        self.assertEqual("warcprox -c {} --certs-dir {} --dedup-db-file /dev/null -d /test -n test -p {} -i".format(
                          w.ca_bundle, w.ca_dir, w.port), w._generate_commandline())

    def test_with(self):
        warc_dir = tempfile.mkdtemp()
        try:
            with warced("test", warc_dir) as w:
                resp = requests.get("http://www.gwu.edu")
                self.assertEqual(200, resp.status_code)
            files = os.listdir(warc_dir)
            self.assertEqual(1, len(files))
            self.assertTrue(files[0].startswith("test"))
            self.assertTrue(files[0].endswith(".warc.gz"))
        finally:
            shutil.rmtree(warc_dir)
