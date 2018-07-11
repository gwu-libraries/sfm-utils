from __future__ import absolute_import
import tests
import tempfile
import shutil
import os
import codecs
import json
from mock import MagicMock
from kombu.message import Message

from sfmutils.consumer import BaseConsumer


class TestableConsumer(BaseConsumer):
    def __init__(self, working_path, raise_exception=False, cause_persist_exception=False):
        BaseConsumer.__init__(self, persist_messages=True, working_path=working_path)
        self.raise_exception = raise_exception
        self.on_message_called = False
        self.on_message_message = None
        self.on_message_routing_key = None
        self.on_message_file_message = None
        if cause_persist_exception:
            self.message_filepath = None
        self.on_persist_exception_called = False

    def on_message(self):
        self.on_message_called = True
        self.on_message_message = self.message
        self.on_message_routing_key = self.routing_key
        with codecs.open(self.message_filepath) as f:
            self.on_message_file_message = json.load(f)
        if self.raise_exception:
            raise Exception

    def on_persist_exception(self, exception):
        self.on_persist_exception_called = True


class TestBaseConsumer(tests.TestCase):

    def setUp(self):
        self.working_path = tempfile.mkdtemp()
        self.message_filepath = os.path.join(self.working_path, "last_message.json")
        self.message = {"key1": "value1"}
        self.routing_key = "test.routing_key"
        self.message_file = {"routing_key": self.routing_key, "message": self.message}

    def tearDown(self):
        if os.path.exists(self.working_path):
            shutil.rmtree(self.working_path)

    def test_callback(self):
        consumer = TestableConsumer(self.working_path)
        mock_mq_message = MagicMock(spec=Message)
        mock_mq_message.delivery_info = {"routing_key": self.routing_key}
        consumer._callback(self.message, mock_mq_message)

        self.assertTrue(consumer.on_message_called)
        self.assertEqual(self.message, consumer.on_message_message)
        self.assertEqual(self.routing_key, consumer.on_message_routing_key)
        self.assertFalse(os.path.exists(self.message_filepath))
        self.assertEqual(self.message_file, consumer.on_message_file_message)

    def test_callback_exception(self):
        consumer = TestableConsumer(self.working_path, raise_exception=True)
        mock_mq_message = MagicMock(spec=Message)
        mock_mq_message.delivery_info = {"routing_key": self.routing_key}
        exception_caught = False
        try:
            consumer._callback(self.message, mock_mq_message)
        except Exception:
            exception_caught = True
        self.assertTrue(exception_caught)
        self.assertTrue(consumer.on_message_called)
        self.assertEqual(self.message, consumer.on_message_message)
        self.assertEqual(self.routing_key, consumer.on_message_routing_key)
        self.assertFalse(os.path.exists(self.message_filepath))
        self.assertEqual(self.message_file, consumer.on_message_file_message)

    def test_callback_persist_exception(self):
        consumer = TestableConsumer(self.working_path, cause_persist_exception=True)
        mock_mq_message = MagicMock(spec=Message)
        mock_mq_message.delivery_info = {"routing_key": self.routing_key}
        consumer._callback(self.message, mock_mq_message)

        self.assertFalse(consumer.on_message_called)
        self.assertTrue(consumer.on_persist_exception_called)
        self.assertIsNotNone(self.message)
        self.assertIsNotNone(self.routing_key)

    def test_message_from_file(self):
        self._write_message_file()
        consumer = TestableConsumer(self.working_path)
        consumer.message_from_file(self.message_filepath)

        self.assertTrue(consumer.on_message_called)
        self.assertEqual(self.message, consumer.on_message_message)
        self.assertEqual(self.routing_key, consumer.on_message_routing_key)
        self.assertTrue(os.path.exists(self.message_filepath))
        self.assertEqual(self.message_file, consumer.on_message_file_message)

    def test_resume_from_file(self):
        self._write_message_file()
        consumer = TestableConsumer(self.working_path)
        consumer.resume_from_file()

        self.assertTrue(consumer.on_message_called)
        self.assertEqual(self.message, consumer.on_message_message)
        self.assertEqual(self.routing_key, consumer.on_message_routing_key)
        self.assertFalse(os.path.exists(self.message_filepath))
        self.assertEqual(self.message_file, consumer.on_message_file_message)

    def _write_message_file(self):
        with codecs.open(self.message_filepath, 'w') as f:
            json.dump(self.message_file, f)
