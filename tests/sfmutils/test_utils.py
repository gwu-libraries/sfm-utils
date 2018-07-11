from __future__ import absolute_import
from sfmutils.utils import safe_string
from unittest import TestCase


class TestUtils(TestCase):
    def test_safe_string(self):
        self.assertEqual("fooBAR12", safe_string("fooBAR12"))
        self.assertEqual("foo-bar-12", safe_string("foo.bar 12", replace_char="-"))
