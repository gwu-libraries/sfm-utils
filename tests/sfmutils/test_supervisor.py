from __future__ import absolute_import
import tempfile
import shutil
import os
import json
import getpass
from unittest import TestCase
from mock import patch, MagicMock
from xmlrpclib import ServerProxy
from sfmutils.supervisor import HarvestSupervisor


class TestHarvestSupervisor(TestCase):

    def setUp(self):
        self.working_path = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.working_path):
            shutil.rmtree(self.working_path)

    @patch("sfmutils.supervisor.xmlrpclib.ServerProxy", autospec=True)
    def test_supervisor_start_and_stop(self, mock_server_proxy_class):
        message = {
            "id": "test:1",
            "collection_set": {
                "id": "test_collection_set",
            }
        }

        conf_path = tempfile.mkdtemp()
        log_path = tempfile.mkdtemp()

        # Setup mocks
        mock_server_proxy1 = MagicMock(spec=ServerProxy)
        mock_supervisor1 = MagicMock()
        mock_server_proxy1.supervisor = mock_supervisor1
        mock_server_proxy2 = MagicMock(spec=ServerProxy)
        mock_supervisor2 = MagicMock()
        mock_server_proxy2.supervisor = mock_supervisor2
        mock_server_proxy3 = MagicMock(spec=ServerProxy)
        mock_supervisor3 = MagicMock()
        mock_server_proxy3.supervisor = mock_supervisor3
        mock_server_proxy4 = MagicMock(spec=ServerProxy)
        mock_supervisor4 = MagicMock()
        mock_server_proxy4.supervisor = mock_supervisor4

        # Return mock_twarc when instantiating a twarc.
        mock_server_proxy_class.side_effect = [mock_server_proxy1, mock_server_proxy2, mock_server_proxy3,
                                               mock_server_proxy4]

        supervisor = HarvestSupervisor("/opt/sfm/test_harvester.py", "test_host", "test_user", "test_password",
                                       self.working_path, conf_path=conf_path, log_path=log_path, debug=True)

        # Conf_path is empty
        self.assertFalse(os.listdir(conf_path))

        # Start (which calls stop first)
        supervisor.start(message, "harvest.start.test.test_search", debug=False, debug_warcprox=True)

        # Seed file contains message.
        with open(os.path.join(conf_path, "test_1.json")) as f:
            seed = json.load(f)
        self.assertDictEqual(message, seed["message"])

        # Conf file as expected
        with open(os.path.join(conf_path, "test_1.conf")) as f:
            conf = f.read()
        self.assertEqual("""[program:test_1]
command=python /opt/sfm/test_harvester.py --debug=False --debug-warcprox=True seed {conf_path}/test_1.json {working_path} --streaming --host test_host --username test_user --password test_password
user={user}
autostart=true
autorestart=unexpected
exitcodes=0,1
stopwaitsecs=900
stderr_logfile={log_path}/test_1.err.log
stdout_logfile={log_path}/test_1.out.log
""".format(conf_path=conf_path, log_path=log_path, user=getpass.getuser(), working_path=self.working_path), conf)

        # Remove process called
        mock_supervisor1.stopProcess.assert_called_once_with("test_1", True)
        mock_supervisor1.removeProcessGroup.assert_called_once_with("test_1")

        # Reload_config called
        mock_supervisor2.reloadConfig.assert_called_once_with()

        # Add process group called
        mock_supervisor3.addProcessGroup.assert_called_once_with("test_1")

        # Now stop
        supervisor.stop("test:1")
        # Remove process called
        mock_supervisor4.stopProcess.assert_called_once_with("test_1", True)
        mock_supervisor4.removeProcessGroup.assert_called_once_with("test_1")

        # Files deleted
        self.assertFalse(os.path.exists(os.path.join(conf_path, "test_1.json")))
        self.assertFalse(os.path.exists(os.path.join(conf_path, "test_1.conf")))

        shutil.rmtree(conf_path)
        shutil.rmtree(log_path)
