from __future__ import absolute_import
import tempfile
import shutil
import os
import json
from unittest import TestCase
from mock import patch, MagicMock
from xmlrpclib import ServerProxy
from sfmutils.supervisor import HarvestSupervisor


class TestHarvestSupervisor(TestCase):

    @patch("sfmutils.supervisor.xmlrpclib.ServerProxy", autospec=True)
    def test_supervisor_start_and_stop(self, mock_server_proxy_class):
        message = {
            "id": "test:1",
            "collection": {
                "id": "test_collection",
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
                                conf_path=conf_path, log_path=log_path)

        # Conf_path is empty
        self.assertFalse(os.listdir(conf_path))

        # Start (which calls stop first)
        supervisor.start(message, "harvest.start.test.test_search")

        # Seed file contains message.
        with open(os.path.join(conf_path, "test_1.json")) as f:
            seed = json.load(f)
        self.assertDictEqual(message, seed)

        # Conf file as expected
        with open(os.path.join(conf_path, "test_1.conf")) as f:
            conf = f.read()
        self.assertEqual("""[program:test_1]
command=python /opt/sfm/test_harvester.py seed {conf_path}/test_1.json --streaming --host test_host --username test_user --password test_password --routing-key harvest.start.test.test_search
user=justinlittman
autostart=true
autorestart=true
stderr_logfile={log_path}/test_1.err.log
stdout_logfile={log_path}/test_1.out.log
""".format(conf_path=conf_path, log_path=log_path), conf)

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
