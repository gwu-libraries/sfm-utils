import tests
from mock import patch, MagicMock, call
from sfmutils.find_warcs import main
from sfmutils.api_client import ApiClient


class TestFindWarcs(tests.TestCase):
    @patch("sfmutils.find_warcs.sys")
    @patch("sfmutils.find_warcs.ApiClient", autospec=True)
    def test_find_warcs(self, mock_api_client_cls, mock_sys):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.collections.side_effect = [[{"collection_id": "abc123"}], [{"collection_id": "def456"}]]
        mock_api_client.warcs.side_effect = [[{"path": "/sfm-data/abc123"}],
                                             [{"path": "/sfm-data/def456"}, {"path": "/sfm-data/def789"}]]

        self.assertEqual("/sfm-data/abc123 /sfm-data/def456 /sfm-data/def789",
                         main("find_warcs.py --debug=True abc def".split(" ")))
        self.assertEqual([call(collection_id_startswith='abc'), call(collection_id_startswith='def')],
                         mock_api_client.collections.call_args_list)
        self.assertEqual(
            [call(harvest_date_end=None, harvest_date_start=None, created_date_start=None,
                  created_date_end=None, collection_id='abc123'),
             call(harvest_date_end=None, harvest_date_start=None, created_date_start=None,
                  created_date_end=None, collection_id='def456')],
            mock_api_client.warcs.call_args_list)
        mock_sys.exit.assert_not_called()

    @patch("sfmutils.find_warcs.sys")
    @patch("sfmutils.find_warcs.ApiClient", autospec=True)
    def test_find_warcs_with_args(self, mock_api_client_cls, mock_sys):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.collections.side_effect = [[{"collection_id": "def456"}]]
        mock_api_client.warcs.side_effect = [[{"path": "/sfm-data/abc123"}],
                                             [{"path": "/sfm-data/def456"}, {"path": "/sfm-data/def789"}]]

        self.assertEqual("/sfm-data/abc123 /sfm-data/def456 /sfm-data/def789",
                         main("find_warcs.py --debug=True --harvest-start 2015-02-22T14:49:07Z --harvest-end "
                              "2016-02-22T14:49:07Z --warc-end 2014-02-22T14:49:07Z --warc-start "
                              "2013-02-22T14:49:07Z abcdefghijklmnopqrstuvwxyz012345 def".split(
                             " ")))
        self.assertEqual([call(collection_id_startswith='def')],
                         mock_api_client.collections.call_args_list)
        self.assertEqual(
            [call(harvest_date_end='2016-02-22T14:49:07Z', harvest_date_start='2015-02-22T14:49:07Z',
                  collection_id='abcdefghijklmnopqrstuvwxyz012345', created_date_end="2014-02-22T14:49:07Z",
                  created_date_start="2013-02-22T14:49:07Z"),
             call(harvest_date_end='2016-02-22T14:49:07Z', harvest_date_start='2015-02-22T14:49:07Z',
                  collection_id='def456', created_date_end="2014-02-22T14:49:07Z",
                  created_date_start="2013-02-22T14:49:07Z")],
            mock_api_client.warcs.call_args_list)
        mock_sys.exit.assert_not_called()

    @patch("sfmutils.find_warcs.sys")
    @patch("sfmutils.find_warcs.ApiClient", autospec=True)
    def test_find_warcs_no_matches(self, mock_api_client_cls, mock_sys):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.collections.side_effect = [[]]

        main("find_warcs.py --debug=True abc".split(" "))
        self.assertEqual([call(collection_id_startswith='abc')],
                         mock_api_client.collections.call_args_list)
        mock_api_client.warcs.assert_not_called()
        mock_sys.exit.assert_called_once_with(1)

    @patch("sfmutils.find_warcs.sys")
    @patch("sfmutils.find_warcs.ApiClient", autospec=True)
    def test_find_warcs_multiple_matches(self, mock_api_client_cls, mock_sys):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.collections.side_effect = [[{"collection_id": "abc123"}, {"collection_id": "abc456"}]]

        main("find_warcs.py --debug=True abc".split(" "))
        self.assertEqual([call(collection_id_startswith='abc')],
                         mock_api_client.collections.call_args_list)
        mock_api_client.warcs.assert_not_called()
        mock_sys.exit.assert_called_once_with(1)
