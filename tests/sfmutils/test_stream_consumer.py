from __future__ import absolute_import
from unittest import TestCase
from mock import MagicMock, patch
import socket
import tempfile
import os
import shutil
from sfmutils.consumer import MqConfig
from sfmutils.stream_consumer import StreamConsumer
from sfmutils.supervisor import HarvestSupervisor


class TestStreamConsumer(TestCase):
    def setUp(self):
        self.patcher = patch("sfmutils.stream_consumer.HarvestSupervisor")
        mock_supervisor_class = self.patcher.start()
        self.mock_supervisor = MagicMock(spec=HarvestSupervisor)
        mock_supervisor_class.side_effect = [self.mock_supervisor]
        self.working_path = tempfile.mkdtemp()
        self.stream_consumer = StreamConsumer("/opt/sfm/test.py", self.working_path,
                                              mq_config=MqConfig(None, None, None, None,
                                                                 {"test_queue": [
                                                                     "harvest.start.test.test_usertimeline",
                                                                     "harvest.start.test.test_search"]}), )

    def tearDown(self):
        self.patcher.stop()
        if os.path.exists(self.working_path):
            shutil.rmtree(self.working_path)

    def test_stop_queue(self):
        stop_queue = "test_queue_{}".format(socket.gethostname())
        self.assertSetEqual({"test_queue", stop_queue},
                            set(self.stream_consumer.mq_config.queues.keys()))
        self.assertListEqual(["harvest.stop.test.test_usertimeline", "harvest.stop.test.test_search"],
                             self.stream_consumer.mq_config.queues[stop_queue])

    def test_start(self):
        message = {
            "id": "test:1",
            "collection_set": {
                "id": "test_collection_set"
            }
        }

        self.stream_consumer.message = message
        self.stream_consumer.routing_key = "harvest.start.test.test_usertimeline"
        self.stream_consumer.on_message()

        self.mock_supervisor.start.called_once_with(message, "harvest.start.test.test_usertimeline")

    def test_stop(self):
        message = {
            "id": "test:1"
        }

        self.stream_consumer.message = message
        self.stream_consumer.routing_key = "harvest.stop.test.test_usertimeline"
        self.stream_consumer.on_message()

        self.mock_supervisor.stop.called_once_with("test:1")
