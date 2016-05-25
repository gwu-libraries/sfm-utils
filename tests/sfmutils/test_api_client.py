from unittest import TestCase
from sfmutils.api_client import ApiClient
import vcr as base_vcr

vcr = base_vcr.VCR(
        cassette_library_dir='tests/fixtures',
        record_mode='once',
    )


class TestApiClient(TestCase):
    def setUp(self):
        self.client = ApiClient("http://192.168.99.100:8081/")

    @vcr.use_cassette()
    def test_all_warcs(self):
        self.assertEqual(3, len(list(self.client.warcs())))

    @vcr.use_cassette()
    def test_warcs_by_seedset(self):
        self.assertEqual(2, len(list(self.client.warcs(seedset_id="005b131f5f854402afa2b08a4b7ba960"))))
        self.assertEqual(0, len(list(self.client.warcs(seedset_id="x005b131f5f854402afa2b08a4b7ba960"))))

    @vcr.use_cassette()
    def test_warcs_by_seed(self):
        self.assertEqual(2, len(list(self.client.warcs(seed_ids="ded3849618b04818ae100a489d67d395"))))
        self.assertEqual(0, len(list(self.client.warcs(seed_ids="xded3849618b04818ae100a489d67d395"))))
        self.assertEqual(2, len(list(self.client.warcs(seed_ids=["ded3849618b04818ae100a489d67d395"]))))
        self.assertEqual(2, len(list(self.client.warcs(seed_ids=["ded3849618b04818ae100a489d67d395", "x"]))))
        self.assertEqual(3, len(
            list(self.client.warcs(seed_ids=["48722ac6154241f592fd74da775b7ab7", "3ce76759a3ee40b894562a35359dfa54"]))))

    @vcr.use_cassette()
    def test_warcs_by_harvest_date_start(self):
        self.assertEqual(3, len(list(self.client.warcs(harvest_date_start="2015-02-22T14:49:07Z"))))
        self.assertEqual(1, len(list(self.client.warcs(harvest_date_start="2016-02-22T14:49:07Z"))))
        self.assertEqual(0, len(list(self.client.warcs(harvest_date_start="2017-02-22T14:48:07Z"))))

    @vcr.use_cassette()
    def test_warcs_by_harvest_date_end(self):
        self.assertEqual(0, len(list(self.client.warcs(harvest_date_end="2015-02-22T14:49:07Z"))))
        self.assertEqual(2, len(list(self.client.warcs(harvest_date_end="2016-02-22T14:49:07Z"))))
        self.assertEqual(3, len(list(self.client.warcs(harvest_date_end="2017-02-22T14:48:07Z"))))

    @vcr.use_cassette()
    def test_exclude_web(self):
        self.assertEqual(4, len(list(self.client.warcs(exclude_web=True))))
        self.assertEqual(5, len(list(self.client.warcs(exclude_web=False))))

    @vcr.use_cassette()
    def test_all_seedsets(self):
        self.assertEqual(5, len(list(self.client.seedsets())))

    @vcr.use_cassette()
    def test_seedsets_startswith(self):
        self.assertEqual(1, len(list(self.client.seedsets(seedset_id_startswith="8fcb71eb883745"))))
        self.assertEqual(0, len(list(self.client.seedsets(seedset_id_startswith="x8fcb71eb883745"))))
