import tests
import os
import tempfile
import shutil
import json
from mock import MagicMock, patch, Mock
import iso8601
from sfmutils.exporter import BaseTable, BaseExporter, CODE_WARC_MISSING, CODE_NO_WARCS, CODE_BAD_REQUEST
from sfmutils.api_client import ApiClient
from sfmutils.warc_iter import IterItem
import datetime
from kombu import Producer, Connection, Exchange


class TestExporter(tests.TestCase):
    def setUp(self):
        self.warc_base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "warcs")
        self.warcs = [{"warc_id": "9dc0b9c3a93a49eb8f713330b43f954c",
                       "path": "test_1-20151202200525007-00000-30033-GLSS-F0G5RP-8000.warc.gz",
                       "sha1": "000ffb3371eadb507d77d181ca3f0c5d3c74a2fc", "bytes": 460518,
                       "date_created": "2016-02-22T14:49:07Z"},
                      {"warc_id": "d3f524b52de0495b9abbf3b36b1fb06f",
                       "path": "test_1-20151202190229530-00000-29525-GLSS-F0G5RP-8000.warc.gz",
                       "sha1": "28076c245bc23d5e18e8531c19700dec869e2f9a",
                       "bytes": 58048,
                       "date_created": "2016-02-22T14:37:26Z"}]
        self.warc_filepaths = [
            os.path.join(self.warc_base_path, "test_1-20151202200525007-00000-30033-GLSS-F0G5RP-8000.warc.gz"),
            os.path.join(self.warc_base_path, "test_1-20151202190229530-00000-29525-GLSS-F0G5RP-8000.warc.gz")]
        self.export_path = tempfile.mkdtemp()
        self.working_path = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.export_path):
            shutil.rmtree(self.export_path)
        if os.path.exists(self.working_path):
            shutil.rmtree(self.working_path)

    @patch("sfmutils.exporter.ApiClient", autospec=True)
    # Mock out Producer
    @patch("sfmutils.consumer.Producer", autospec=True)
    def test_export_collection(self, mock_producer_cls, mock_api_client_cls):
        mock_warc_iter_cls = MagicMock()
        mock_table_cls = MagicMock()
        mock_table = MagicMock(spec=BaseTable)
        mock_table_cls.side_effect = [mock_table]
        mock_table.__iter__ = Mock(return_value=iter([[("key1", "key2"), ("k1v1", "k2v1"), ("k1v2", "k2v2")], ]))

        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.warcs.side_effect = [self.warcs]

        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_producer = MagicMock(spec=Producer)
        mock_producer_cls.return_value = mock_producer

        item_date_start = "2007-01-25T12:00:00Z"
        item_datetime_start = iso8601.parse_date(item_date_start)
        item_date_end = "2008-02-25T12:00:00Z"
        item_datetime_end = iso8601.parse_date(item_date_end)
        harvest_date_start = "2007-03-25T12:00:00Z"
        harvest_date_end = "2008-04-25T12:00:00Z"

        export_message = {
            "id": "test1",
            "type": "test_user",
            "collection": {
                "id": "005b131f5f854402afa2b08a4b7ba960"
            },
            "format": "csv",
            "segment_size": None,
            "path": self.export_path,
            "dedupe": True,
            "item_date_start": item_date_start,
            "item_date_end": item_date_end,
            "harvest_date_start": harvest_date_start,
            "harvest_date_end": harvest_date_end,

        }

        exporter = BaseExporter("http://test", mock_warc_iter_cls, mock_table_cls, self.working_path,
                                warc_base_path=self.warc_base_path, host="testhost")
        exporter.mq_config = True
        exporter._producer_connection = mock_connection
        exporter.exchange = mock_exchange

        exporter.routing_key = "export.start.test.test_user"
        exporter.message = export_message
        exporter.on_message()

        mock_api_client_cls.assert_called_once_with("http://test")
        mock_api_client.warcs.assert_called_once_with(exclude_web=True,
                                                      collection_id="005b131f5f854402afa2b08a4b7ba960",
                                                      seed_ids=[], harvest_date_start=harvest_date_start,
                                                      harvest_date_end=harvest_date_end)
        mock_table_cls.assert_called_once_with(self.warc_filepaths, True, item_datetime_start,
                                               item_datetime_end, [], None)

        self.assertTrue(exporter.result.success)
        csv_filepath = os.path.join(self.export_path, "test1_001.csv")
        self.assertTrue(os.path.exists(csv_filepath))
        with open(csv_filepath, "r") as f:
            lines = f.readlines()
        self.assertEqual(3, len(lines))

        name, _, kwargs = mock_producer.mock_calls[0]
        self.assertEqual("publish", name)
        self.assertEqual("export.status.test.test_user", kwargs["routing_key"])
        export_status_message = kwargs["body"]
        self.assertEqual("running", export_status_message["status"])
        self.assertTrue(iso8601.parse_date(export_status_message["date_started"]))
        self.assertEqual("test1", export_status_message["id"])
        self.assertEqual("Base Exporter", export_status_message["service"])
        self.assertEqual("testhost", export_status_message["host"])
        self.assertTrue(export_status_message["instance"])

        name, _, kwargs = mock_producer.mock_calls[1]
        self.assertEqual("publish", name)
        self.assertEqual("export.status.test.test_user", kwargs["routing_key"])
        export_status_message = kwargs["body"]
        self.assertEqual("completed success", export_status_message["status"])
        self.assertTrue(iso8601.parse_date(export_status_message["date_started"]))
        self.assertTrue(iso8601.parse_date(export_status_message["date_ended"]))
        self.assertEqual("test1", export_status_message["id"])
        self.assertEqual("Base Exporter", export_status_message["service"])
        self.assertEqual("testhost", export_status_message["host"])
        self.assertTrue(export_status_message["instance"])

    @patch("sfmutils.exporter.ApiClient", autospec=True)
    # Mock out Producer
    @patch("sfmutils.consumer.Producer", autospec=True)
    def test_export_dehydrate(self, mock_producer_cls, mock_api_client_cls):
        mock_warc_iter_cls = MagicMock()
        mock_table_cls = MagicMock()
        mock_table = MagicMock(spec=BaseTable)
        mock_table_cls.side_effect = [mock_table]
        mock_table.__iter__ = Mock(return_value=iter([[("key1", "key2"), ("k1v1", "k2v1"), ("k1v2", "k2v2")], ]))
        mock_table.id_field.return_value = "key2"

        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.warcs.side_effect = [self.warcs]

        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_producer = MagicMock(spec=Producer)
        mock_producer_cls.return_value = mock_producer

        export_message = {
            "id": "test1",
            "type": "test_user",
            "collection": {
                "id": "005b131f5f854402afa2b08a4b7ba960"
            },
            "format": "dehydrate",
            "segment_size": None,
            "path": self.export_path,

        }

        exporter = BaseExporter("http://test", mock_warc_iter_cls, mock_table_cls, self.working_path,
                                warc_base_path=self.warc_base_path, host="testhost")
        exporter.mq_config = True
        exporter._producer_connection = mock_connection
        exporter.exchange = mock_exchange

        exporter.routing_key = "export.start.test.test_user"
        exporter.message = export_message
        exporter.on_message()

        mock_api_client_cls.assert_called_once_with("http://test")
        mock_api_client.warcs.assert_called_once_with(exclude_web=True,
                                                      collection_id="005b131f5f854402afa2b08a4b7ba960",
                                                      seed_ids=[], harvest_date_end=None, harvest_date_start=None)
        mock_table_cls.assert_called_once_with(self.warc_filepaths, False, None, None, [], None)

        self.assertTrue(exporter.result.success)
        txt_filepath = os.path.join(self.export_path, "test1_001.txt")
        self.assertTrue(os.path.exists(txt_filepath))
        with open(txt_filepath, "r") as f:
            lines = f.readlines()
        self.assertEqual(2, len(lines))
        self.assertEqual("k2v1\n", lines[0])

        name, _, kwargs = mock_producer.mock_calls[1]
        self.assertEqual("publish", name)
        self.assertEqual("export.status.test.test_user", kwargs["routing_key"])
        export_status_message = kwargs["body"]
        self.assertEqual("completed success", export_status_message["status"])
        self.assertEqual("test1", export_status_message["id"])

    @patch("sfmutils.exporter.ApiClient", autospec=True)
    def test_export_seeds(self, mock_api_client_cls):
        mock_warc_iter_cls = MagicMock()
        mock_table_cls = MagicMock()
        mock_table = MagicMock(spec=BaseTable)
        mock_table_cls.side_effect = [mock_table]
        mock_table.__iter__ = Mock(return_value=iter([[("key1", "key2"), ("k1v1", "k2v1"), ("k1v2", "k2v2")], ]))

        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.warcs.side_effect = [self.warcs]

        export_message = {
            "id": "test2",
            "type": "test_user",
            "seeds": [
                {
                    "id": "005b131f5f854402afa2b08a4b7ba960",
                    "uid": "uid1"
                },
                {
                    "id": "105b131f5f854402afa2b08a4b7ba960",
                    "uid": "uid2"
                },
            ],
            "format": "csv",
            "segment_size": None,
            "path": self.export_path,
        }

        exporter = BaseExporter("http://test", mock_warc_iter_cls, mock_table_cls, self.working_path,
                                warc_base_path=self.warc_base_path, host="testhost")

        exporter.routing_key = "export.start.test.test_user"
        exporter.message = export_message
        exporter.on_message()

        mock_api_client_cls.assert_called_once_with("http://test")
        mock_api_client.warcs.assert_called_once_with(exclude_web=True, collection_id=None,
                                                      seed_ids=["005b131f5f854402afa2b08a4b7ba960",
                                                                "105b131f5f854402afa2b08a4b7ba960"],
                                                      harvest_date_start=None, harvest_date_end=None)
        mock_table_cls.assert_called_once_with(self.warc_filepaths, False, None, None, ["uid1", "uid2"], None)

        self.assertTrue(exporter.result.success)
        csv_filepath = os.path.join(self.export_path, "test2_001.csv")
        self.assertTrue(os.path.exists(csv_filepath))
        with open(csv_filepath, "r") as f:
            lines = f.readlines()
        self.assertEqual(3, len(lines))

    @patch("sfmutils.exporter.ApiClient", autospec=True)
    def test_export_collection_missing_warc(self, mock_api_client_cls):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]

        export_message = {
            "id": "test3",
            "type": "test_user",
            "collection": {
                "id": "005b131f5f854402afa2b08a4b7ba960"
            },
            "seeds": [
                {
                    "id": "005b131f5f854402afa2b08a4b7ba960",
                    "uid": "uid1"
                }
            ],
            "format": "csv",
            "segment_size": None,
            "path": self.export_path
        }

        exporter = BaseExporter("http://test", None, None, self.working_path, warc_base_path=self.warc_base_path,
                                host="testhost")

        exporter.routing_key = "export.start.test.test_user"
        exporter.message = export_message
        exporter.on_message()

        mock_api_client_cls.assert_called_once_with("http://test")

        self.assertFalse(exporter.result.success)
        self.assertEqual(CODE_BAD_REQUEST, exporter.result.errors[0].code)

    @patch("sfmutils.exporter.ApiClient", autospec=True)
    # Mock out Producer
    @patch("sfmutils.consumer.Producer", autospec=True)
    def test_export_collection_and_seeds(self, mock_producer_cls, mock_api_client_cls):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        warcs = [{"warc_id": "9dc0b9c3a93a49eb8f713330b43f954c",
                  "path": "xtest_1-20151202165907873-00000-306-60892de9dfc6-8001.warc.gz",
                  "sha1": "000ffb3371eadb507d77d181ca3f0c5d3c74a2fc", "bytes": 460518,
                  "date_created": "2016-02-22T14:49:07Z"}]
        mock_api_client.warcs.side_effect = [warcs]

        mock_connection = MagicMock(spec=Connection)
        mock_exchange = MagicMock(spec=Exchange)
        mock_exchange.name = "test exchange"
        mock_producer = MagicMock(spec=Producer)
        mock_producer_cls.return_value = mock_producer

        export_message = {
            "id": "test2",
            "type": "test_user",
            "collection": {
                "id": "005b131f5f854402afa2b08a4b7ba960"
            },
            "format": "csv",
            "segment_size": None,
            "path": self.export_path
        }

        exporter = BaseExporter("http://test", None, None, self.working_path, warc_base_path=self.warc_base_path,
                                host="testhost")
        exporter.mq_config = True
        exporter._producer_connection = mock_connection
        exporter.exchange = mock_exchange

        exporter.routing_key = "export.start.test.test_user"
        exporter.message = export_message
        exporter.on_message()

        mock_api_client_cls.assert_called_once_with("http://test")
        mock_api_client.warcs.assert_called_once_with(exclude_web=True,
                                                      collection_id="005b131f5f854402afa2b08a4b7ba960",
                                                      seed_ids=[], harvest_date_end=None, harvest_date_start=None)

        self.assertFalse(exporter.result.success)

        name, _, kwargs = mock_producer.mock_calls[1]
        self.assertEqual("publish", name)
        self.assertEqual("export.status.test.test_user", kwargs["routing_key"])
        export_status_message = kwargs["body"]
        self.assertEqual("completed failure", export_status_message["status"])
        self.assertTrue(iso8601.parse_date(export_status_message["date_started"]))
        self.assertTrue(iso8601.parse_date(export_status_message["date_ended"]))
        self.assertEqual("test2", export_status_message["id"])
        self.assertTrue(CODE_WARC_MISSING, export_status_message["errors"][0]["code"])
        self.assertTrue(CODE_NO_WARCS, export_status_message["errors"][0]["code"])

    def test_export_full_json(self):
        mock_warc_iter_cls = MagicMock()
        mock_warc_iter = MagicMock()
        mock_warc_iter_cls.side_effect = [mock_warc_iter]
        mock_warc_iter.iter.return_value = [
            IterItem(None, None, None, None, {"key1": "k1v1", "key2": "k2v1", "key3": "k3v1"}),
            IterItem(None, None, None, None, {"key1": "k1v2", "key2": "k2v2", "key3": "k3v2"})]

        export_filepath = os.path.join(self.export_path, "test")
        now = datetime.datetime.now()
        limit_uids = [11, 14]

        exporter = BaseExporter(None, mock_warc_iter_cls, None, self.working_path, warc_base_path=self.warc_base_path,
                                host="testhost")

        exporter._full_json_export(self.warcs, export_filepath, True, now, None, limit_uids, None)

        mock_warc_iter_cls.assert_called_once_with(self.warcs, limit_uids)
        mock_warc_iter.iter.assert_called_once_with(dedupe=True, item_date_start=now, item_date_end=None,
                                                    limit_item_types=None)

        file_path = export_filepath + '_001.json'
        self.assertTrue(os.path.exists(file_path))
        with open(file_path, "r") as f:
            lines = f.readlines()
        self.assertEqual(2, len(lines))
        self.assertDictEqual({"key1": "k1v1", "key2": "k2v1", "key3": "k3v1"}, json.loads(lines[0]))

    def test_export_full_json_segment(self):
        mock_warc_iter_cls = MagicMock()
        mock_warc_iter = MagicMock()
        mock_warc_iter_cls.side_effect = [mock_warc_iter]
        mock_warc_iter.iter.return_value = [
            IterItem(None, None, None, None, {"key1": "k1v1", "key2": "k2v1", "key3": "k3v1"}),
            IterItem(None, None, None, None, {"key1": "k1v2", "key2": "k2v2", "key3": "k3v2"}),
            IterItem(None, None, None, None, {"key1": "k1v3", "key2": "k2v3", "key3": "k3v3"}),
            IterItem(None, None, None, None, {"key1": "k1v4", "key2": "k2v4", "key3": "k3v4"}),
            IterItem(None, None, None, None, {"key1": "k1v5", "key2": "k2v5", "key3": "k3v5"}),
            IterItem(None, None, None, None, {"key1": "k1v6", "key2": "k2v6", "key3": "k3v6"}),
            IterItem(None, None, None, None, {"key1": "k1v7", "key2": "k2v7", "key3": "k3v7"})]

        export_filepath = os.path.join(self.export_path, "test")
        now = datetime.datetime.now()
        limit_uids = [11, 14]

        exporter = BaseExporter(None, mock_warc_iter_cls, None, self.working_path, warc_base_path=self.warc_base_path,
                                host="testhost")

        exporter._full_json_export(self.warcs, export_filepath, True, now, None, limit_uids, 3)

        mock_warc_iter_cls.assert_called_once_with(self.warcs, limit_uids)
        mock_warc_iter.iter.assert_called_once_with(dedupe=True, item_date_start=now, item_date_end=None,
                                                    limit_item_types=None)

        # file test_1.json, test_2.json , test_3.json
        for idx in xrange(3):
            file_path = export_filepath + '_' + str(idx + 1).zfill(3) + '.json'
            self.assertTrue(os.path.exists(file_path))
            with open(file_path, "r") as f:
                lines = f.readlines()
            # the test_3.json only has 1 row
            if idx == 2:
                self.assertEqual(1, len(lines))
            else:
                self.assertEqual(3, len(lines))
            self.assertDictEqual(
                {"key1": "k1v" + str(1 + idx * 3), "key2": "k2v" + str(1 + idx * 3), "key3": "k3v" + str(1 + idx * 3)},
                json.loads(lines[0]))


class TestableTable(BaseTable):
    def _header_row(self):
        return "key1", "key2", "key3"

    def _row(self, item):
        return item["key1"], item["key2"], item["key3"]


class TestBaseTable(tests.TestCase):
    def setUp(self):
        self.warc_paths = ("/collection_set1/warc1.warc.gz", "/collection_set1/warc2.warc.gz")

    def test_table(self):

        mock_warc_iter_cls = MagicMock()
        mock_warc_iter = MagicMock()
        mock_warc_iter_cls.side_effect = [mock_warc_iter]
        mock_warc_iter.iter.return_value = [
            IterItem(None, None, None, None, {"key1": "k1v1", "key2": "k2v1", "key3": "k3v1"}),
            IterItem(None, None, None, None, {"key1": "k1v2", "key2": "k2v2", "key3": "k3v2"}),
            IterItem(None, None, None, None, {"key1": "k1v3", "key2": "k2v3", "key3": "k3v3"}),
            IterItem(None, None, None, None, {"key1": "k1v4", "key2": "k2v4", "key3": "k3v4"}),
            IterItem(None, None, None, None, {"key1": "k1v5", "key2": "k2v5", "key3": "k3v5"}),
            IterItem(None, None, None, None, {"key1": "k1v6", "key2": "k2v6", "key3": "k3v6"}),
            IterItem(None, None, None, None, {"key1": "k1v7", "key2": "k2v7", "key3": "k3v7"})]
        now = datetime.datetime.now()
        limit_uids = [11, 14]

        tables = TestableTable(self.warc_paths, True, now, None, limit_uids, mock_warc_iter_cls, segment_row_size=2)
        chunk_cnt = 0
        for idx, table in enumerate(tables):
            chunk_cnt += 1
            for count, row in enumerate(table):
                # every chunk should start with header row
                if count == 0:
                    # Header row
                    # Just testing first and last, figuring these might change often.
                    self.assertEqual("key1", row[0])
                    self.assertEqual("key2", row[1])
                    self.assertEqual("key3", row[2])
                # chunk 1 and row 2
                if idx == 0 and count == 1:
                    # First row
                    self.assertEqual("k1v1", row[0])
                    self.assertEqual("k2v1", row[1])
                    self.assertEqual("k3v1", row[2])
                # chunk 3 and row 3
                if idx == 2 and count == 2:
                    self.assertEqual("k1v6", row[0])
                    self.assertEqual("k2v6", row[1])
                    self.assertEqual("k3v6", row[2])
                # chunk 4 and row 2
                if idx == 3 and count == 1:
                    self.assertEqual("k1v7", row[0])
                    self.assertEqual("k2v7", row[1])
                    self.assertEqual("k3v7", row[2])

        self.assertEqual(4, chunk_cnt)

        mock_warc_iter_cls.assert_called_with(self.warc_paths, limit_uids)
        mock_warc_iter.iter.assert_called_once_with(dedupe=True, item_date_end=None, item_date_start=now,
                                                    limit_item_types=None)
