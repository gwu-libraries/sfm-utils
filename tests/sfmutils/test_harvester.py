from __future__ import absolute_import
from mock import MagicMock, patch, PropertyMock
import json
import tempfile
import os
import shutil
from time import sleep
from kombu import Producer, Connection, Exchange
from kombu.message import Message
import iso8601
import logging
import codecs
from tests import TestCase
from datetime import date, timedelta
from sfmutils.harvester import BaseHarvester, STATUS_RUNNING, STATUS_FAILURE, STATUS_SUCCESS, STATUS_STOPPING, \
    CODE_HARVEST_RESUMED, CODE_UNKNOWN_ERROR
from sfmutils.state_store import JsonHarvestStateStore
from sfmutils.harvester import Msg
from sfmutils.warcprox import warced

log = logging.getLogger(__name__)

WARC_FILENAME_TEMPLATE = "test_1-20151109195229879-{0:05d}-97528-GLSS-F0G5RP-8000.warc.gz"


def write_fake_warc(path, filename):
    if not os.path.exists(path):
        os.makedirs(path)
    with open(os.path.join(path, filename), "w") as f:
        f.write("Fake warc")


def write_message_file(message):
    # Write message to file
    fd, message_filepath = tempfile.mkstemp()
    f = os.fdopen(fd, "w")
    json.dump({
        "routing_key": "harvest.start.test.test_usertimeline",
        "message": message
    }, f)
    f.close()
    return message_filepath


class TestableHarvester(BaseHarvester):
    def __init__(self, working_path, connection=None, exchange=None, raise_exception_on_count=(0,), shutdown_on_count=0,
                 tries=1):
        BaseHarvester.__init__(self, working_path, stream_restart_interval_secs=5, warc_rollover_secs=120, tries=tries,
                               host="localhost")
        if connection:
            self.mq_config = True
            self._producer_connection = connection
        self.exchange = exchange
        self.harvest_seed_call_count = 0
        self.process_warc_call_count = 0
        # Throw an exception on a particular loop of harvest_seeds
        self.raise_exception_on_count = raise_exception_on_count
        # Trigger shutdown on a particular loop of harvest_seeds
        self.shutdown_on_count = shutdown_on_count
        # This means it is streaming.
        if self.shutdown_on_count > 0:
            self.is_streaming = True

    def harvest_seeds(self):
        self.harvest_seed_call_count += 1
        log.debug("Harvest seed call count is %s", self.harvest_seed_call_count)
        if self.harvest_seed_call_count == 1:
            self.result.infos.append(Msg("FAKE_CODE1", "This is my message."))
            self.result.warnings.append(Msg("FAKE_CODE2", "This is my warning."))
            self.result.errors.append(Msg("FAKE_CODE3", "This is my error."))
            self.result.token_updates["131866249@N02"] = "j.littman"
            self.result.uids["library_of_congress"] = "671366249@N03"

        if self.harvest_seed_call_count in self.raise_exception_on_count:
            raise Exception("Darn!")

        # Write a fake warc file
        write_fake_warc(self.warc_temp_dir, WARC_FILENAME_TEMPLATE.format(
            self.harvest_seed_call_count))
        if self.is_streaming:
            while not self.stop_harvest_seeds_event.is_set():
                sleep(.5)

        if self.shutdown_on_count == self.harvest_seed_call_count:
            self.stop_harvest_loop_event.set()
            self.stop_harvest_seeds_event.set()
            if self.restart_stream_timer:
                self.restart_stream_timer.cancel()
            if self.queue_warc_files_timer:
                self.queue_warc_files_timer.cancel()

    def process_warc(self, warc_filepath):
        self.process_warc_call_count += 1
        log.debug("Process warc call count is %s", self.process_warc_call_count)

        if self.process_warc_call_count == 1:
            self.result.increment_stats("stuff", count=5, day=date.today() - timedelta(days=1))
        self.result.increment_stats("stuff", count=10)
        self.state_store.set_state("testable_harvester", "stuff.last", self.process_warc_call_count)


class TestBaseHarvester(TestCase):
    def setUp(self):
        self.working_path = tempfile.mkdtemp()
        self.harvest_path = tempfile.mkdtemp()
        self.message = {
            "id": "test:1",
            "type": "test_type",
            "path": self.harvest_path,
            "collection_set": {
                "id": "test_collection_set"
            },
            "collection": {
                "id": "test_collection"
            }
        }

    def tearDown(self):
        if os.path.exists(self.working_path):
            shutil.rmtree(self.working_path)
        if os.path.exists(self.harvest_path):
            shutil.rmtree(self.harvest_path)

    def assert_warcs_moved(self, start, stop):
        for i in range(start, stop):
            self.assertTrue(
                os.path.exists(os.path.join(self.harvest_path, "2015/11/09/19", WARC_FILENAME_TEMPLATE.format(i))))

    def assert_warc_created_message(self, warc_number, name, _, kwargs):
        self.assertEqual("warc_created", kwargs["routing_key"])
        warc_created_message = kwargs["body"]
        self.assertEqual(warc_created_message["harvest"]["id"], "test:1")
        self.assertEqual(warc_created_message["harvest"]["type"], "test_type")
        self.assertEqual(warc_created_message["collection_set"]["id"], "test_collection_set")
        self.assertEqual(warc_created_message["collection"]["id"], "test_collection")
        self.assertEqual(warc_created_message["warc"]["path"],
                         os.path.join(self.harvest_path,
                                      "2015/11/09/19", WARC_FILENAME_TEMPLATE.format(warc_number)))
        self.assertEqual(warc_created_message["warc"]["sha1"], "3d63d3c46d5dfac8495621c9c697e2089e5359b2")
        self.assertEqual(warc_created_message["warc"]["bytes"], 9)
        self.assertEqual(32, len(warc_created_message["warc"]["id"]))
        self.assertIsNotNone(iso8601.parse_date(warc_created_message["warc"]["date_created"]))

    def assert_first_running_harvest_status(self, name, _, kwargs, is_resume=False):
        # Running harvest result message
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs["routing_key"])
        harvest_result_message = kwargs["body"]
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], STATUS_RUNNING)
        if not is_resume:
            self.assertEqual(0, len(harvest_result_message["infos"]))
            self.assertEqual(0, len(harvest_result_message["warnings"]))
            self.assertEqual(0, len(harvest_result_message["errors"]))
        else:
            self.assertEqual(1, len(harvest_result_message["infos"]))
            self.assertEqual(2, len(harvest_result_message["warnings"]))
            self.assertEqual(CODE_HARVEST_RESUMED, harvest_result_message["warnings"][1]["code"])
            self.assertEqual(1, len(harvest_result_message["errors"]))
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_started"]))
        self.assertIsNone(harvest_result_message.get("date_ended"))
        if is_resume:
            self.assertEqual(2, harvest_result_message["warcs"]["count"])
            self.assertEqual(18, harvest_result_message["warcs"]["bytes"])
            self.assertDictEqual({
                (date.today() - timedelta(days=1)).isoformat(): {
                    "stuff": 10
                }
            }, harvest_result_message["stats"])
        else:
            self.assertEqual(0, harvest_result_message["warcs"]["count"])
            self.assertEqual(0, harvest_result_message["warcs"]["bytes"])
            self.assertDictEqual({}, harvest_result_message["stats"])

    def assert_second_running_harvest_status(self, name, _, kwargs, is_resume=False):
        # Running harvest result message
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs["routing_key"])
        harvest_result_message = kwargs["body"]
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], STATUS_RUNNING)
        if not is_resume:
            self.assertEqual(1, len(harvest_result_message["infos"]))
            self.assertDictEqual({
                "code": "FAKE_CODE1",
                "message": "This is my message."
            }, harvest_result_message["infos"][0])
            self.assertEqual(1, len(harvest_result_message["warnings"]))
            self.assertDictEqual({
                "code": "FAKE_CODE2",
                "message": "This is my warning."
            }, harvest_result_message["warnings"][0])
            self.assertEqual(1, len(harvest_result_message["errors"]))
            self.assertDictEqual({
                "code": "FAKE_CODE3",
                "message": "This is my error."
            }, harvest_result_message["errors"][0])
        else:
            self.assertEqual(2, len(harvest_result_message["infos"]))
            self.assertEqual(3, len(harvest_result_message["warnings"]))
            self.assertEqual(2, len(harvest_result_message["errors"]))
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_started"]))
        self.assertIsNone(harvest_result_message.get("date_ended"))
        self.assertDictEqual({
            "131866249@N02": "j.littman"
        }, harvest_result_message["token_updates"])
        self.assertDictEqual({
            "library_of_congress": "671366249@N03"
        }, harvest_result_message["uids"])
        self.assertEqual(1 + (2 if is_resume else 0), harvest_result_message["warcs"]["count"])
        self.assertEqual(9 * (1 + (2 if is_resume else 0)), harvest_result_message["warcs"]["bytes"])
        self.assertDictEqual({
            (date.today() - timedelta(days=1)).isoformat(): {
                "stuff": 5 + (10 if is_resume else 0)
            },
            date.today().isoformat(): {
                "stuff": 10,
            }
        }, harvest_result_message["stats"])

    def assert_running_harvest_status(self, warc_count, name, _, kwargs, is_resume=False, status=STATUS_RUNNING):
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs["routing_key"])
        harvest_result_message = kwargs["body"]
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], status)
        if not is_resume:
            self.assertEqual(1, len(harvest_result_message["infos"]))
            self.assertEqual(1, len(harvest_result_message["warnings"]))
            self.assertEqual(1, len(harvest_result_message["errors"]))
        else:
            self.assertEqual(2, len(harvest_result_message["infos"]))
            self.assertEqual(3, len(harvest_result_message["warnings"]))
            self.assertEqual(2, len(harvest_result_message["errors"]))
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_started"]))
        self.assertIsNone(harvest_result_message.get("date_ended"))
        self.assertDictEqual({}, harvest_result_message["token_updates"])
        self.assertDictEqual({}, harvest_result_message["uids"])
        self.assertEqual(warc_count + (2 if is_resume else 0), harvest_result_message["warcs"]["count"])
        self.assertEqual((warc_count + (2 if is_resume else 0)) * 9, harvest_result_message["warcs"]["bytes"])
        self.assertDictEqual({
            (date.today() - timedelta(days=1)).isoformat(): {
                "stuff": 5 + (10 if is_resume else 0)
            },
            date.today().isoformat(): {
                "stuff": warc_count * 10,
            }
        }, harvest_result_message["stats"])

    def assert_stopping_harvest_status(self, warc_count, name, _, kwargs, is_resume=False):
        self.assert_running_harvest_status(warc_count, name, _, kwargs, is_resume=is_resume, status=STATUS_STOPPING)

    def assert_completed_harvest_status(self, warc_count, name, _, kwargs, is_resume=False):
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs["routing_key"])
        harvest_result_message = kwargs["body"]
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], STATUS_SUCCESS)
        if not is_resume:
            self.assertEqual(1, len(harvest_result_message["infos"]))
            self.assertEqual(1, len(harvest_result_message["warnings"]))
            self.assertEqual(1, len(harvest_result_message["errors"]))
        else:
            self.assertEqual(2, len(harvest_result_message["infos"]))
            self.assertEqual(3, len(harvest_result_message["warnings"]))
            self.assertEqual(2, len(harvest_result_message["errors"]))
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_started"]))
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_ended"]))
        self.assertDictEqual({}, harvest_result_message["token_updates"])
        self.assertDictEqual({}, harvest_result_message["uids"])
        self.assertEqual(warc_count + (2 if is_resume else 0), harvest_result_message["warcs"]["count"])
        self.assertEqual((warc_count + (2 if is_resume else 0)) * 9, harvest_result_message["warcs"]["bytes"])
        self.assertDictEqual({
            (date.today() - timedelta(days=1)).isoformat(): {
                "stuff": 5 + (10 if is_resume else 0)
            },
            date.today().isoformat(): {
                "stuff": warc_count * 10,
            }
        }, harvest_result_message["stats"])

    def assert_state_store(self, value):
        state_store = JsonHarvestStateStore(self.harvest_path)
        self.assertEqual(value, state_store.get_state("testable_harvester", "stuff.last"))

    # Mock out warcprox.
    @patch("sfmutils.harvester.warced", autospec=True)
    # Mock out Producer
    @patch("sfmutils.consumer.ConsumerProducerMixin.producer", new_callable=PropertyMock, spec=Producer)
    def test_consume(self, mock_producer, mock_warced_class):
        # Setup
        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_warced = MagicMock(spec=warced)
        mock_warced_class.side_effect = [mock_warced]
        mock_message = MagicMock(spec=Message)
        mock_message.delivery_info = {"routing_key": "harvest.start.test.test_usertimeline"}

        # Create harvester and invoke _callback
        harvester = TestableHarvester(self.working_path, mock_connection, mock_exchange)
        harvester._callback(self.message, mock_message)

        # Test assertions
        self.assertEqual(1, harvester.harvest_seed_call_count)
        self.assertEqual(1, harvester.process_warc_call_count)

        mock_warced_class.assert_called_once_with("test_1", harvester.warc_temp_dir, debug=False, interrupt=False,
                                                  rollover_time=120)
        self.assertTrue(mock_warced.__enter__.called)
        self.assertTrue(mock_warced.__exit__.called)

        # Warc path deleted
        self.assertFalse(os.path.exists(harvester.warc_temp_dir))

        # Warcs moved
        # This tests 2015/11/09/19/test_1-20151109195229879-00001-97528-GLSS-F0G5RP-8000.warc.gz
        self.assert_warcs_moved(1, 2)

        # Messages
        self.assert_first_running_harvest_status(*mock_producer.mock_calls[1])
        self.assert_warc_created_message(1, *mock_producer.mock_calls[3])

        # The first one has errors, infos, warnings, token updates, uids
        self.assert_second_running_harvest_status(*mock_producer.mock_calls[5])

        self.assert_completed_harvest_status(1, *mock_producer.mock_calls[7])

        # Check state store
        self.assert_state_store(1)

    @patch("sfmutils.harvester.warced", autospec=True)
    @patch("sfmutils.consumer.ConsumerProducerMixin.producer", new_callable=PropertyMock, spec=Producer)
    def test_stream_consume(self, mock_producer, mock_warced_class):
        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_warced = MagicMock(spec=warced)
        mock_warced_class.return_value = mock_warced
        mock_message = MagicMock(spec=Message)
        mock_message.delivery_info = {"routing_key": "harvest.start.test.test_usertimeline"}

        # Create harvester and invoke _callback
        harvester = TestableHarvester(self.working_path, mock_connection, mock_exchange, shutdown_on_count=5)
        harvester._callback(self.message, mock_message)

        # Test assertions
        self.assertEqual(5, harvester.harvest_seed_call_count)
        self.assertEqual(5, harvester.process_warc_call_count)

        mock_warced_class.assert_called_with("test_1", harvester.warc_temp_dir, debug=False, interrupt=True,
                                             rollover_time=None)
        self.assertEqual(5, mock_warced_class.call_count)

        # Warcs moved
        self.assert_warcs_moved(1, 6)

        # Check state store
        self.assert_state_store(5)

    @patch("sfmutils.harvester.warced", autospec=True)
    @patch("sfmutils.consumer.ConsumerProducerMixin.producer", new_callable=PropertyMock, spec=Producer)
    def test_stream_harvest_from_file_and_resume(self, mock_producer, mock_warced_class):
        # This is really the same as stream_consume, just invoked from file.
        # Also testing resuming a harvest.

        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_warced = MagicMock(spec=warced)
        mock_warced_class.return_value = mock_warced
        mock_message = MagicMock(spec=Message)
        mock_message.delivery_info = {"routing_key": "harvest.start.test.test_usertimeline"}

        # Write message to file
        message_filepath = write_message_file(self.message)

        # Create harvester
        harvester = TestableHarvester(self.working_path, mock_connection, mock_exchange, shutdown_on_count=5)
        # Create a WARC file. This is a WARC file that was created during a previous harvest
        # that was interrupted before it was processed.
        write_fake_warc(os.path.join(self.working_path, "tmp/test_1"), WARC_FILENAME_TEMPLATE.format(0))

        # Write a result file
        yesterday = (date.today() - timedelta(days=1))
        result_filepath = os.path.join(self.working_path, "test_1_result.json")
        with codecs.open(result_filepath, "w") as f:
            json.dump({
                "warcs": ["warc1.warc.gz", "warc2.warc.gz"],
                "warc_bytes": 18,
                "stats": [[yesterday.isoformat(), {"stuff": 10}]],
                "started": yesterday.isoformat(),
                "infos": [{"code": "FAKE_CODE1", "message": "This is my previous message."}],
                "warnings": [{"code": "FAKE_CODE2", "message": "This is my previous warning."}],
                "errors": [{"code": "FAKE_CODE3", "message": "This is my previous error."}]
            }, f)

        # Invoke harvest_from_file
        harvester.harvest_from_file(message_filepath, is_streaming=True, delete=True)

        # # Test assertions
        self.assertEqual(5, harvester.harvest_seed_call_count)
        self.assertEqual(6, harvester.process_warc_call_count)

        mock_warced_class.assert_called_with("test_1", harvester.warc_temp_dir, debug=False, interrupt=True,
                                             rollover_time=None)
        self.assertEqual(5, mock_warced_class.call_count)

        self.assertFalse(os.path.exists(message_filepath))

        # Warcs moved
        self.assert_warcs_moved(0, 6)

        self.assertFalse(os.path.exists(result_filepath))

    @patch("sfmutils.harvester.warced", autospec=True)
    @patch("sfmutils.consumer.ConsumerProducerMixin.producer", new_callable=PropertyMock, spec=Producer)
    def test_consume_with_exception(self, mock_producer, mock_warced_class):

        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_warced = MagicMock(spec=warced)
        mock_warced_class.return_value = mock_warced
        mock_message = MagicMock(spec=Message)
        mock_message.delivery_info = {"routing_key": "harvest.start.test.test_usertimeline"}

        # Create harvester and invoke _callback
        harvester = TestableHarvester(self.working_path, mock_connection, mock_exchange,
                                      raise_exception_on_count=[1, 2], tries=2)
        harvester._callback(self.message, mock_message)

        # Test assertions
        self.assertEqual(2, harvester.harvest_seed_call_count)
        self.assertEqual(0, harvester.process_warc_call_count)

        # Failed harvest result message
        self.assertEqual(4, len(mock_producer.mock_calls))
        self.assert_first_running_harvest_status(*mock_producer.mock_calls[1])
        name, _, kwargs = mock_producer.mock_calls[3]
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs["routing_key"])
        harvest_result_message = kwargs["body"]
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], STATUS_FAILURE)
        self.assertEqual(1, len(harvest_result_message["infos"]))
        self.assertEqual(1, len(harvest_result_message["warnings"]))
        self.assertEqual(2, len(harvest_result_message["errors"]))
        self.assertEqual(CODE_UNKNOWN_ERROR, harvest_result_message["errors"][1]["code"])
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_started"]))
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_ended"]))

    # Mock out warcprox.
    @patch("sfmutils.harvester.warced", autospec=True)
    # Mock out Producer
    @patch("sfmutils.consumer.ConsumerProducerMixin.producer", new_callable=PropertyMock, spec=Producer)
    def test_consume_with_transient_exception(self, mock_producer, mock_warced_class):
        # Setup
        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_warced = MagicMock(spec=warced)
        mock_warced_class.side_effect = [mock_warced, mock_warced]
        mock_message = MagicMock(spec=Message)
        mock_message.delivery_info = {"routing_key": "harvest.start.test.test_usertimeline"}

        # Create harvester and invoke _callback
        harvester = TestableHarvester(self.working_path, mock_connection, mock_exchange, raise_exception_on_count=[1],
                                      tries=2)
        harvester._callback(self.message, mock_message)

        # Test assertions
        self.assertEqual(2, harvester.harvest_seed_call_count)
        self.assertEqual(1, harvester.process_warc_call_count)

        mock_warced_class.assert_called_with("test_1", harvester.warc_temp_dir, debug=False, interrupt=False,
                                             rollover_time=120)
        self.assertTrue(mock_warced.__enter__.called)
        self.assertTrue(mock_warced.__exit__.called)

        # Warc path deleted
        self.assertFalse(os.path.exists(harvester.warc_temp_dir))

        # Warcs moved
        # This tests 2015/11/09/19/test_1-20151109195229879-00001-97528-GLSS-F0G5RP-8000.warc.gz
        self.assert_warcs_moved(2, 3)

        # Messages
        self.assert_first_running_harvest_status(*mock_producer.mock_calls[1])
        self.assert_warc_created_message(2, *mock_producer.mock_calls[3])

        # The first one has errors, infos, warnings, token updates, uids
        self.assert_second_running_harvest_status(*mock_producer.mock_calls[5])

        self.assert_completed_harvest_status(1, *mock_producer.mock_calls[7])

        # Check state store
        self.assert_state_store(1)

    # Mock out Producer
    @patch("sfmutils.consumer.ConsumerProducerMixin.producer", new_callable=PropertyMock, spec=Producer)
    def test_on_persist_exception(self, mock_producer):
        # Setup
        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"

        # Create harvester and invoke _callback
        harvester = TestableHarvester(self.working_path, mock_connection, mock_exchange)
        # harvester._callback(self.message, mock_message)
        harvester.message = self.message
        harvester.routing_key = "harvest.start.test.test_usertimeline"
        harvester.on_persist_exception(Exception("Problem persisting"))

        # Running harvest result message
        (name, _, kwargs) = mock_producer.mock_calls[1]
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs["routing_key"])
        harvest_result_message = kwargs["body"]
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], STATUS_FAILURE)
        self.assertEqual(1, len(harvest_result_message["errors"]))
        self.assertIsNotNone(iso8601.parse_date(harvest_result_message["date_started"]))

    def test_list_warcs(self):
        harvester = BaseHarvester(self.working_path, host="localhost")
        write_fake_warc(self.working_path, "test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz")
        write_fake_warc(self.working_path, "test_1-20151109195229879-00001-97528-GLSS-F0G5RP-8000.warc")
        write_fake_warc(self.working_path, "test_1-20151109195229879-00002-97528-GLSS-F0G5RP-8000")
        os.mkdir(os.path.join(self.working_path, "test_1-20151109195229879-00003-97528-GLSS-F0G5RP-8000.warc.gz"))
        warc_dirs = harvester._list_warcs(self.working_path)
        self.assertSetEqual({"test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz",
                             "test_1-20151109195229879-00001-97528-GLSS-F0G5RP-8000.warc"},
                            set(warc_dirs))
