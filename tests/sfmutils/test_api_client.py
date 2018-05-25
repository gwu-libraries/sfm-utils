from unittest import TestCase
from sfmutils.api_client import ApiClient
import vcr as base_vcr

vcr = base_vcr.VCR(
        cassette_library_dir='tests/fixtures',
        record_mode='once',
    )
# Make sure to set page size to 2 when recording test fixtures.


class TestApiClient(TestCase):
    def setUp(self):
        self.client = ApiClient("http://localhost:8080/")

    @vcr.use_cassette()
    def test_all_warcs(self):
        self.assertEqual(3, len(list(self.client.warcs())))

    @vcr.use_cassette()
    def test_warcs_by_collection(self):
        self.assertEqual(2, len(list(self.client.warcs(collection_id="366439dbb28146a9bd439dcc3f076c70"))))
        self.assertEqual(0, len(list(self.client.warcs(collection_id="x366439dbb28146a9bd439dcc3f076c70"))))

    @vcr.use_cassette()
    def test_warcs_by_seed(self):
        self.assertEqual(2, len(list(self.client.warcs(seed_ids="4117a0b5c42646589f5dc81b0fa5eb0c"))))
        self.assertEqual(0, len(list(self.client.warcs(seed_ids="x4117a0b5c42646589f5dc81b0fa5eb0c"))))
        self.assertEqual(2, len(list(self.client.warcs(seed_ids=["4117a0b5c42646589f5dc81b0fa5eb0c"]))))
        self.assertEqual(2, len(list(self.client.warcs(seed_ids=["4117a0b5c42646589f5dc81b0fa5eb0c", "x"]))))
        self.assertEqual(3, len(
            list(self.client.warcs(seed_ids=["4117a0b5c42646589f5dc81b0fa5eb0c", "c07e9e180dd24abcac700d1934bda3d1"]))))

    @vcr.use_cassette()
    def test_warcs_by_harvest_date_start(self):
        self.assertEqual(3, len(list(self.client.warcs(harvest_date_start="2017-05-25T13:57:47.980000Z"))))
        self.assertEqual(1, len(list(self.client.warcs(harvest_date_start="2018-05-25T13:56:47.980000Z"))))
        self.assertEqual(0, len(list(self.client.warcs(harvest_date_start="2019-05-25T13:57:47.980000Z"))))

    @vcr.use_cassette()
    def test_warcs_by_warc_created_date(self):
        self.assertEqual(3, len(list(self.client.warcs(harvest_date_start="2017-05-25T13:57:47.980000Z"))))
        self.assertEqual(1, len(list(self.client.warcs(harvest_date_start="2018-05-25T13:56:47.980000Z"))))
        self.assertEqual(0, len(list(self.client.warcs(harvest_date_start="2019-05-25T13:57:47.980000Z"))))

    @vcr.use_cassette()
    def test_all_collections(self):
        self.assertEqual(2, len(list(self.client.collections())))

    @vcr.use_cassette()
    def test_collections_startswith(self):
        self.assertEqual(1, len(list(self.client.collections(collection_id_startswith="366439dbb"))))
        self.assertEqual(0, len(list(self.client.collections(collection_id_startswith="x366439dbb"))))
