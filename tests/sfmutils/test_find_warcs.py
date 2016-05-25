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
        mock_api_client.seedsets.side_effect = [[{"seedset_id": "abc123"}], [{"seedset_id": "def456"}]]
        mock_api_client.warcs.side_effect = [[{"path": "/sfm-data/abc123"}],
                                             [{"path": "/sfm-data/def456"}, {"path": "/sfm-data/def789"}]]

        self.assertEqual("/sfm-data/abc123 /sfm-data/def456 /sfm-data/def789",
                         main("find_warcs.py --debug=True abc def".split(" ")))
        self.assertEqual([call(seedset_id_startswith='abc'), call(seedset_id_startswith='def')],
                         mock_api_client.seedsets.call_args_list)
        self.assertEqual([call(exclude_web=True, harvest_date_end=None, harvest_date_start=None, seedset_id='abc123'),
                          call(exclude_web=True, harvest_date_end=None, harvest_date_start=None, seedset_id='def456')],
                         mock_api_client.warcs.call_args_list)
        mock_sys.exit.assert_not_called()

    @patch("sfmutils.find_warcs.sys")
    @patch("sfmutils.find_warcs.ApiClient", autospec=True)
    def test_find_warcs_with_args(self, mock_api_client_cls, mock_sys):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.seedsets.side_effect = [[{"seedset_id": "def456"}]]
        mock_api_client.warcs.side_effect = [[{"path": "/sfm-data/abc123"}],
                                             [{"path": "/sfm-data/def456"}, {"path": "/sfm-data/def789"}]]

        self.assertEqual("/sfm-data/abc123 /sfm-data/def456 /sfm-data/def789",
                         main("find_warcs.py --debug=True --harvest-start 2015-02-22T14:49:07Z --harvest-end "
                              "2016-02-22T14:49:07Z --include-web abcdefghijklmnopqrstuvwxyz012345 def".split(" ")))
        self.assertEqual([call(seedset_id_startswith='def')],
                         mock_api_client.seedsets.call_args_list)
        self.assertEqual(
            [call(exclude_web=False, harvest_date_end='2016-02-22T14:49:07Z', harvest_date_start='2015-02-22T14:49:07Z',
                  seedset_id='abcdefghijklmnopqrstuvwxyz012345'),
             call(exclude_web=False, harvest_date_end='2016-02-22T14:49:07Z', harvest_date_start='2015-02-22T14:49:07Z',
                  seedset_id='def456')],
            mock_api_client.warcs.call_args_list)
        mock_sys.exit.assert_not_called()

    @patch("sfmutils.find_warcs.sys")
    @patch("sfmutils.find_warcs.ApiClient", autospec=True)
    def test_find_warcs_no_matches(self, mock_api_client_cls, mock_sys):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.seedsets.side_effect = [[]]

        main("find_warcs.py --debug=True abc".split(" "))
        self.assertEqual([call(seedset_id_startswith='abc')],
                         mock_api_client.seedsets.call_args_list)
        mock_api_client.warcs.assert_not_called()
        mock_sys.exit.assert_called_once_with(1)

    @patch("sfmutils.find_warcs.sys")
    @patch("sfmutils.find_warcs.ApiClient", autospec=True)
    def test_find_warcs_multiple_matches(self, mock_api_client_cls, mock_sys):
        mock_api_client = MagicMock(spec=ApiClient)
        mock_api_client_cls.side_effect = [mock_api_client]
        mock_api_client.seedsets.side_effect = [[{"seedset_id": "abc123"}, {"seedset_id": "abc456"}]]

        main("find_warcs.py --debug=True abc".split(" "))
        self.assertEqual([call(seedset_id_startswith='abc')],
                         mock_api_client.seedsets.call_args_list)
        mock_api_client.warcs.assert_not_called()
        mock_sys.exit.assert_called_once_with(1)
