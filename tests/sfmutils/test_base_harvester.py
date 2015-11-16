from __future__ import absolute_import
from mock import MagicMock, patch
import pika
import json
import tempfile
import os
import shutil
import threading
from time import sleep
from unittest import TestCase
from sfmutils.harvester import BaseHarvester
from sfmutils.state_store import NullHarvestStateStore
from sfmutils.harvester import Msg, MqConfig
from sfmutils.warcprox import warced


def fake_warc(path, filename):
    with open(os.path.join(path, filename), "w") as f:
        f.write("Fake warc")


class TestableHarvester(BaseHarvester):
    def __init__(self, state_store, warc_dir):
        BaseHarvester.__init__(self)
        self.state_store = state_store
        self.warc_dir = warc_dir

    def harvest_seeds(self):
        #Write a fake warc file
        fake_warc(self.warc_dir, "test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz")
        self.harvest_result.infos.append(Msg("FAKE_CODE1", "This is my message."))
        self.harvest_result.warnings.append(Msg("FAKE_CODE2", "This is my warning."))
        self.harvest_result.errors.append(Msg("FAKE_CODE3", "This is my error."))
        self.harvest_result.urls.extend(("http://www.gwu.edu", "http://library.gwu.edu"))
        self.harvest_result.increment_summary("photo", increment=12)
        self.harvest_result.increment_summary("user")
        self.harvest_result.token_updates["131866249@N02"] = "j.littman"
        self.harvest_result.uids["library_of_congress"] = "671366249@N03"

    def _create_state_store(self):
        pass


class ExceptionRaisingHarvester(BaseHarvester):
    def __init__(self):
        BaseHarvester.__init__(self)
        self.mq_config = MqConfig(None, None, None, "test_exchange", None, None)

    def harvest_seeds(self):
        raise Exception("Darn!")


class TestableStreamHarvester(BaseHarvester):
    def __init__(self, state_store, warc_dir):
        BaseHarvester.__init__(self, process_interval_secs=3)
        self.state_store = state_store
        self.warc_dir = warc_dir
        self.mq_config = MqConfig(None, None, None, "test_exchange", None, None)

    def harvest_seeds(self):
        self.harvest_result.infos.append(Msg("FAKE_CODE1", "This is my message."))
        self.harvest_result.warnings.append(Msg("FAKE_CODE2", "This is my warning."))
        self.harvest_result.errors.append(Msg("FAKE_CODE3", "This is my error."))
        self.harvest_result.token_updates["131866249@N02"] = "j.littman"
        self.harvest_result.uids["library_of_congress"] = "671366249@N03"
        i = 0
        while not self.stop_event.is_set():
            i += 1
            if i % 4 == 0:
                fake_warc(self.warc_dir, "test_1-20151109195229879-{0:05d}-97528-GLSS-F0G5RP-8000.warc.gz".format(i))
            #Lock before updating
            with self.harvest_result_lock:
                self.harvest_result.urls.append("http://www.{}.edu".format(i))
                self.harvest_result.increment_summary("stuff")

            sleep(.5)

    def _create_state_store(self):
        pass


class TestBaseHarvester(TestCase):

    #Mock out tempfile so that have control over location of warc directory.
    @patch("sfmutils.harvester.tempfile", autospec=True)
    #Mock out warcprox.
    @patch("sfmutils.harvester.warced", autospec=True)
    def test_consume(self, mock_warced_class, mock_tempfile):
        test_collection_path = tempfile.mkdtemp()
        #Setup
        message = {
            "id": "test:1",
            "collection": {
                "id": "test_collection",
                "path": test_collection_path
            }
        }
        mock_channel = MagicMock(spec=pika.channel.Channel)
        mock_method = MagicMock(spec=pika.frame.Method)
        mock_method.delivery_tag = 1
        mock_method.routing_key = "harvest.start.test.test_usertimeline"
        mock_state_store = MagicMock(spec=NullHarvestStateStore)
        test_warc_path = tempfile.mkdtemp()
        mock_tempfile.mkdtemp.return_value = test_warc_path
        mock_warced = MagicMock(spec=warced)
        #Return mock_twarc when instantiating a twarc.
        mock_warced_class.side_effect = [mock_warced]

        #Create harvester and invoke _callback
        harvester = TestableHarvester(mock_state_store, test_warc_path)
        harvester.mq_config = MqConfig(None, None, None, "test_exchange", None, None)
        harvester._callback(mock_channel, mock_method, None, json.dumps(message))

        #Test assertions
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        self.assertEqual(message, harvester.message)
        mock_tempfile.mkdtemp.assert_called_once_with(prefix="test_1")
        mock_warced_class.assert_called_once_with("test_1", test_warc_path)
        self.assertTrue(mock_warced.__enter__.called)
        self.assertTrue(mock_warced.__exit__.called)

        #Warc path deleted
        self.assertFalse(os.path.exists(test_warc_path))

        #Warcs moved
        self.assertTrue(os.path.exists(
            os.path.join(test_collection_path,
                         "2015/11/09/19/test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz")))
        shutil.rmtree(test_collection_path)

        #Web harvest
        name1, _, kwargs1 = mock_channel.mock_calls[1]
        self.assertEqual("basic_publish", name1)
        self.assertEqual("harvest.start.web", kwargs1["routing_key"])
        web_harvest_message = {
            u"id": u"TestableHarvester:test:1",
            u"parent_id": u"test:1",
            u"type": u"web",
            u"seeds": [
                {
                    u"token": u"http://www.gwu.edu"
                },
                {
                    u"token": u"http://library.gwu.edu"
                }
            ],
            u"collection": {
                u"id": u"test_collection",
                u"path": test_collection_path
            }
        }
        self.assertDictEqual(web_harvest_message, json.loads(kwargs1["body"]))

        #Warc created message
        name2, _, kwargs2 = mock_channel.mock_calls[2]
        self.assertEqual("basic_publish", name2)
        self.assertEqual("test_exchange", kwargs2["exchange"])
        self.assertEqual("warc_created", kwargs2["routing_key"])
        warc_created_message = json.loads(kwargs2["body"])
        self.assertEqual(warc_created_message["collection"]["id"], "test_collection")
        self.assertEqual(warc_created_message["collection"]["path"], test_collection_path)
        self.assertEqual(warc_created_message["warc"]["path"],
                         os.path.join(test_collection_path,
                                      "2015/11/09/19/test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz"))
        self.assertEqual(warc_created_message["warc"]["sha1"], "3d63d3c46d5dfac8495621c9c697e2089e5359b2")
        self.assertEqual(warc_created_message["warc"]["bytes"], 9)
        self.assertEqual(warc_created_message["warc"]["id"], "test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000")
        self.assertIsNotNone(warc_created_message["warc"]["date_created"])

        #Harvest result message
        name3, _, kwargs3 = mock_channel.mock_calls[3]
        self.assertEqual("basic_publish", name3)
        self.assertEqual("test_exchange", kwargs3["exchange"])
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs3["routing_key"])
        harvest_result_message = json.loads(kwargs3["body"])
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], "completed success")
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
        self.assertIsNotNone(harvest_result_message["date_started"])
        self.assertIsNotNone(harvest_result_message["date_ended"])
        self.assertDictEqual({
            "photo": 12,
            "user": 1
        }, harvest_result_message["summary"])
        self.assertDictEqual({
            "131866249@N02": "j.littman"
        }, harvest_result_message["token_updates"])
        self.assertDictEqual({
            "library_of_congress": "671366249@N03"
        }, harvest_result_message["uids"])

    #Mock out tempfile so that have control over location of warc directory.
    @patch("sfmutils.harvester.tempfile", autospec=True)
    #Mock out warcprox.
    @patch("sfmutils.harvester.warced", autospec=True)
    def test_consume_with_exception(self, mock_warced_class, mock_tempfile):
        test_collection_path = tempfile.mkdtemp()
        #Setup
        message = {
            "id": "test:1",
            "collection": {
                "id": "test_collection",
                "path": test_collection_path
            }
        }
        mock_channel = MagicMock(spec=pika.channel.Channel)
        mock_method = MagicMock(spec=pika.frame.Method)
        mock_method.delivery_tag = 1
        mock_method.routing_key = "harvest.start.test.test_usertimeline"
        test_warc_path = tempfile.mkdtemp()
        mock_tempfile.mkdtemp.return_value = test_warc_path
        mock_warced = MagicMock(spec=warced)
        #Return mock_twarc when instantiating a twarc.
        mock_warced_class.side_effect = [mock_warced]

        #Create harvester and invoke _callback
        harvester = ExceptionRaisingHarvester()
        harvester._callback(mock_channel, mock_method, None, json.dumps(message))

        #Test assertions
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        mock_tempfile.mkdtemp.assert_called_once_with(prefix="test_1")
        mock_warced_class.assert_called_once_with("test_1", test_warc_path)
        self.assertTrue(mock_warced.__enter__.called)
        self.assertTrue(mock_warced.__exit__.called)

        #Warc path deleted
        self.assertFalse(os.path.exists(test_warc_path))

        #Harvest result message
        name1, _, kwargs1 = mock_channel.mock_calls[1]
        self.assertEqual("basic_publish", name1)
        self.assertEqual("test_exchange", kwargs1["exchange"])
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs1["routing_key"])
        harvest_result_message = json.loads(kwargs1["body"])
        self.assertEqual(harvest_result_message["id"], "test:1")
        self.assertEqual(harvest_result_message["status"], "completed failure")
        self.assertEqual(1, len(harvest_result_message["errors"]))
        self.assertDictEqual({
            "code": "unknown_error",
            "message": "Darn!"
        }, harvest_result_message["errors"][0])

    #Mock out tempfile so that have control over location of warc directory.
    @patch("sfmutils.harvester.tempfile", autospec=True)
    #Mock out warcprox.
    @patch("sfmutils.harvester.warced", autospec=True)
    def test_harvest_from_file(self, mock_warced_class, mock_tempfile):
        test_collection_path = tempfile.mkdtemp()
        #Setup
        message = {
            "id": "test:1",
            "collection": {
                "id": "test_collection",
                "path": test_collection_path
            }
        }

        #Write message to file
        fd, message_filepath = tempfile.mkstemp()
        f = os.fdopen(fd, "w")
        json.dump(message, f)
        f.close()

        mock_state_store = MagicMock(spec=NullHarvestStateStore)
        test_warc_path = tempfile.mkdtemp()
        mock_tempfile.mkdtemp.return_value = test_warc_path
        mock_warced = MagicMock(spec=warced)
        #Return mock_twarc when instantiating a twarc.
        mock_warced_class.side_effect = [mock_warced]

        #Create harvester and invoke harvest
        harvester = TestableHarvester(mock_state_store, test_warc_path)
        harvester.harvest_from_file(message_filepath)

        #Test assertions
        self.assertEqual(message, harvester.message)
        mock_tempfile.mkdtemp.assert_called_once_with(prefix="test_1")
        mock_warced_class.assert_called_once_with("test_1", test_warc_path)
        self.assertTrue(mock_warced.__enter__.called)
        self.assertTrue(mock_warced.__exit__.called)

        #Warc path deleted
        self.assertFalse(os.path.exists(test_warc_path))

        #Warcs moved
        self.assertTrue(os.path.exists(
            os.path.join(test_collection_path,
                         "2015/11/09/19/test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz")))
        shutil.rmtree(test_collection_path)

        #Test harvest result message
        self.assertTrue(harvester.harvest_result.success)
        self.assertSetEqual({"http://www.gwu.edu", "http://library.gwu.edu"}, harvester.harvest_result.urls_as_set())
        self.assertEqual(1, len(harvester.harvest_result.infos))
        self.assertDictEqual({
            "code": "FAKE_CODE1",
            "message": "This is my message."
        }, harvester.harvest_result.infos[0].to_map())
        self.assertEqual(1, len(harvester.harvest_result.warnings))
        self.assertDictEqual({
            "code": "FAKE_CODE2",
            "message": "This is my warning."
        }, harvester.harvest_result.warnings[0].to_map())
        self.assertEqual(1, len(harvester.harvest_result.errors))
        self.assertDictEqual({
            "code": "FAKE_CODE3",
            "message": "This is my error."
        }, harvester.harvest_result.errors[0].to_map())
        self.assertIsNotNone(harvester.harvest_result.started)
        self.assertIsNotNone(harvester.harvest_result.ended)
        self.assertDictEqual({
            "photo": 12,
            "user": 1
        }, harvester.harvest_result.summary)
        self.assertDictEqual({
            "131866249@N02": "j.littman"
        }, harvester.harvest_result.token_updates)
        self.assertDictEqual({
            "library_of_congress": "671366249@N03"
        }, harvester.harvest_result.uids)
        self.assertListEqual([os.path.join(test_collection_path,
                              "2015/11/09/19/test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz")],
                             harvester.harvest_result.warcs)

        #Delete message file
        os.remove(message_filepath)

    def test_list_warcs(self):
        harvester = BaseHarvester()
        warc_dir = tempfile.mkdtemp()
        fake_warc(warc_dir, "test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz")
        fake_warc(warc_dir, "test_1-20151109195229879-00001-97528-GLSS-F0G5RP-8000.warc")
        fake_warc(warc_dir, "test_1-20151109195229879-00002-97528-GLSS-F0G5RP-8000")
        os.mkdir(os.path.join(warc_dir, "test_1-20151109195229879-00003-97528-GLSS-F0G5RP-8000.warc.gz"))
        try:
            warc_dirs = harvester._list_warcs(warc_dir)
            self.assertSetEqual({"test_1-20151109195229879-00000-97528-GLSS-F0G5RP-8000.warc.gz",
                                 "test_1-20151109195229879-00001-97528-GLSS-F0G5RP-8000.warc"},
                                set(warc_dirs))
        finally:
            shutil.rmtree(warc_dir)

    #Mock out tempfile so that have control over location of warc directory.
    @patch("sfmutils.harvester.tempfile", autospec=True)
    #Mock out warcprox.
    @patch("sfmutils.harvester.warced", autospec=True)
    def test_stream(self, mock_warced_class, mock_tempfile):
        test_collection_path = tempfile.mkdtemp()
        #Setup
        message = {
            "id": "test:1",
            "collection": {
                "id": "test_collection",
                "path": test_collection_path
            }
        }

        #Write message to file
        fd, message_filepath = tempfile.mkstemp()
        f = os.fdopen(fd, "w")
        json.dump(message, f)
        f.close()

        mock_channel = MagicMock(spec=pika.channel.Channel)
        mock_connection = MagicMock(spec=pika.connection.Connection)
        mock_connection.channel.return_value = mock_channel
        mock_state_store = MagicMock(spec=NullHarvestStateStore)
        test_warc_path = tempfile.mkdtemp()
        mock_tempfile.mkdtemp.return_value = test_warc_path
        mock_warced = MagicMock(spec=warced)
        #Return mock_twarc when instantiating a twarc.
        mock_warced_class.side_effect = [mock_warced]

        #Setup interrupt
        def stop_it(h):
            h.stop_event.set()

        #Create harvester and invoke harvest
        harvester = TestableStreamHarvester(mock_state_store, test_warc_path)
        harvester._connection = mock_connection
        t = threading.Timer(5, stop_it, args=[harvester])
        t.start()
        harvester.harvest_from_file(message_filepath, "harvest.start.test.test_usertimeline", is_streaming=True)

        #Test assertions
        # mock_channel.basic_ack.assert_called_once_with(delivery_tag=1)
        mock_connection.channel.assert_called_once_with()
        self.assertEqual(message, harvester.message)
        mock_tempfile.mkdtemp.assert_called_once_with(prefix="test_1")
        mock_warced_class.assert_called_once_with("test_1", test_warc_path)
        self.assertTrue(mock_warced.__enter__.called)
        self.assertTrue(mock_warced.__exit__.called)

        #Warc path deleted
        self.assertFalse(os.path.exists(test_warc_path))

        #Warcs moved
        self.assertTrue(os.path.exists(
            os.path.join(test_collection_path,
                         "2015/11/09/19/test_1-20151109195229879-00004-97528-GLSS-F0G5RP-8000.warc.gz")))
        self.assertTrue(os.path.exists(
            os.path.join(test_collection_path,
                         "2015/11/09/19/test_1-20151109195229879-00008-97528-GLSS-F0G5RP-8000.warc.gz")))
        shutil.rmtree(test_collection_path)

        #Web harvest
        name1, _, kwargs1 = mock_channel.mock_calls[0]
        self.assertEqual("basic_publish", name1)
        self.assertEqual("harvest.start.web", kwargs1["routing_key"])
        web_harvest_message1 = json.loads(kwargs1["body"])
        self.assertEqual("TestableStreamHarvester:test:1", web_harvest_message1["id"])
        self.assertEqual("test:1", web_harvest_message1["parent_id"])
        self.assertEqual("web", web_harvest_message1["type"])
        self.assertEqual("test_collection", web_harvest_message1["collection"]["id"])
        self.assertEqual(test_collection_path, web_harvest_message1["collection"]["path"])
        #Contains some token
        self.assertTrue(len(web_harvest_message1["seeds"]))
        self.assertTrue(web_harvest_message1["seeds"][0]["token"].startswith("http://www."))

        #Warc created message
        name2, _, kwargs2 = mock_channel.mock_calls[1]
        self.assertEqual("basic_publish", name2)
        self.assertEqual("test_exchange", kwargs2["exchange"])
        self.assertEqual("warc_created", kwargs2["routing_key"])
        warc_created_message = json.loads(kwargs2["body"])
        self.assertEqual(warc_created_message["collection"]["id"], "test_collection")
        self.assertEqual(warc_created_message["collection"]["path"], test_collection_path)
        self.assertEqual(warc_created_message["warc"]["path"],
                         os.path.join(test_collection_path,
                                      "2015/11/09/19/test_1-20151109195229879-00004-97528-GLSS-F0G5RP-8000.warc.gz"))
        self.assertEqual(warc_created_message["warc"]["sha1"], "3d63d3c46d5dfac8495621c9c697e2089e5359b2")
        self.assertEqual(warc_created_message["warc"]["bytes"], 9)
        self.assertEqual(warc_created_message["warc"]["id"], "test_1-20151109195229879-00004-97528-GLSS-F0G5RP-8000")
        self.assertIsNotNone(warc_created_message["warc"]["date_created"])

        #Harvest status message
        name3, _, kwargs3 = mock_channel.mock_calls[2]
        self.assertEqual("basic_publish", name3)
        self.assertEqual("test_exchange", kwargs3["exchange"])
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs3["routing_key"])
        harvest_running_message = json.loads(kwargs3["body"])
        self.assertEqual(harvest_running_message["id"], "test:1")
        self.assertEqual(harvest_running_message["status"], "running")
        self.assertEqual(1, len(harvest_running_message["infos"]))
        self.assertDictEqual({
            "code": "FAKE_CODE1",
            "message": "This is my message."
        }, harvest_running_message["infos"][0])
        self.assertEqual(1, len(harvest_running_message["warnings"]))
        self.assertDictEqual({
            "code": "FAKE_CODE2",
            "message": "This is my warning."
        }, harvest_running_message["warnings"][0])
        self.assertEqual(1, len(harvest_running_message["errors"]))
        self.assertDictEqual({
            "code": "FAKE_CODE3",
            "message": "This is my error."
        }, harvest_running_message["errors"][0])
        self.assertIsNotNone(harvest_running_message["date_started"])
        self.assertIsNone(harvest_running_message.get("date_ended"))
        stuff_count = harvest_running_message["summary"]["stuff"]
        self.assertTrue(stuff_count)
        self.assertDictEqual({
            "131866249@N02": "j.littman"
        }, harvest_running_message["token_updates"])
        self.assertDictEqual({
            "library_of_congress": "671366249@N03"
        }, harvest_running_message["uids"])

        #Web harvest
        name4, _, kwargs4 = mock_channel.mock_calls[3]
        self.assertEqual("harvest.start.web", kwargs4["routing_key"])
        web_harvest_message2 = json.loads(kwargs4["body"])
        self.assertEqual("TestableStreamHarvester:test:1", web_harvest_message2["id"])
        #Contains some token
        self.assertTrue(len(web_harvest_message2["seeds"]))
        self.assertTrue(web_harvest_message2["seeds"][0]["token"].startswith("http://www."))

        #Warc created message
        name5, _, kwargs5 = mock_channel.mock_calls[4]
        self.assertEqual("warc_created", kwargs5["routing_key"])
        warc_created_message2 = json.loads(kwargs5["body"])
        self.assertEqual(warc_created_message2["warc"]["path"],
                         os.path.join(test_collection_path,
                                      "2015/11/09/19/test_1-20151109195229879-00008-97528-GLSS-F0G5RP-8000.warc.gz"))
        self.assertEqual(warc_created_message2["warc"]["id"], "test_1-20151109195229879-00008-97528-GLSS-F0G5RP-8000")

        #Harvest completed message
        name6, _, kwargs6 = mock_channel.mock_calls[5]
        self.assertEqual("harvest.status.test.test_usertimeline", kwargs6["routing_key"])
        harvest_completed_message = json.loads(kwargs6["body"])
        self.assertEqual(harvest_completed_message["status"], "completed success")
        self.assertFalse(len(harvest_completed_message["infos"]))
        self.assertFalse(len(harvest_completed_message["warnings"]))
        self.assertFalse(len(harvest_completed_message["errors"]))
        self.assertIsNotNone(harvest_completed_message["date_started"])
        self.assertIsNotNone(harvest_completed_message.get("date_ended"))
        self.assertTrue(stuff_count < harvest_completed_message["summary"]["stuff"])
        self.assertFalse(len(harvest_completed_message["token_updates"]))
        self.assertFalse(len(harvest_completed_message["uids"]))

        #Delete message file
        os.remove(message_filepath)
