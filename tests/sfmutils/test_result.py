from __future__ import absolute_import
from sfmutils.result import Msg
from unittest import TestCase


class TestMgs(TestCase):
    def test_to_map(self):
        self.assertEqual({'code': 'test_code', 'message': 'test_message'},
                         Msg('test_code', 'test_message').to_map())
        self.assertEqual({'code': 'test_code', 'message': 'test_message', 'extra': 'test_extra'},
                         Msg('test_code', 'test_message', extra='test_extra').to_map())
