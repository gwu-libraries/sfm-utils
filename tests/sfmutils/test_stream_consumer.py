from __future__ import absolute_import
from unittest import TestCase
from mock import MagicMock, patch
import json
import socket
from sfmutils.stream_consumer import StreamConsumer
from sfmutils.harvester import MqConfig
from sfmutils.supervisor import Supervisor


class TestStreamConsumer(TestCase):
    def setUp(self):
        self.patcher = patch("sfmutils.stream_consumer.Supervisor")
        mock_supervisor_class = self.patcher.start()
        self.mock_supervisor = MagicMock(spec=Supervisor)
        mock_supervisor_class.side_effect = [self.mock_supervisor]
        self.stream_consumer = StreamConsumer(MqConfig(None, None, None, None,
                                                       {"test_queue": [
                                                           "harvest.start.test.test_usertimeline",
                                                           "harvest.start.test.test_search"]},
                                                       skip_connection=True), "/opt/sfm/test.py")

    def tearDown(self):
        self.patcher.stop()

    def test_stop_queue(self):
        stop_queue = "test_queue_{}".format(socket.gethostname())
        self.assertSetEqual({"test_queue", stop_queue},
                            set(self.stream_consumer.mq_config.queues.keys()))
        self.assertListEqual(["harvest.stop.test.test_usertimeline", "harvest.stop.test.test_search"],
                             self.stream_consumer.mq_config.queues[stop_queue])

    def test_start(self):
        message = {
            "id": "test:1",
            "collection": {
                "id": "test_collection"
            }
        }

        self.stream_consumer.message_body = json.dumps(message)
        self.stream_consumer.routing_key = "harvest.start.test.test_usertimeline"
        self.stream_consumer.harvest()

        self.mock_supervisor.start.called_once_with(message, "harvest.start.test.test_usertimeline")

    def test_stop(self):
        message = {
            "id": "test:1"
        }

        self.stream_consumer.message_body = json.dumps(message)
        self.stream_consumer.routing_key = "harvest.stop.test.test_usertimeline"
        self.stream_consumer.harvest()

        self.mock_supervisor.stop.called_once_with("test:1")
