from __future__ import absolute_import
from unittest import TestCase
import os
from sfmutils.warc_iter import BaseWarcIter


class TestableNotLineOrientedWarcIter(BaseWarcIter):
    def _select_record(self, url):
        return True

    def _item_iter(self, url, json_obj):
            for status in json_obj["statuses"]:
                yield "twitter_status", status

    def _select_record(self, url):
        return url.startswith("https://api.twitter.com/1.1")


class TestableLineOrientedWarcIter(BaseWarcIter):
    def __init__(self, filepath):
        BaseWarcIter.__init__(self, filepath)

    def _select_record(self, url):
        return True

    def _item_iter(self, url, json_obj):
        yield "twitter_status", json_obj

    @property
    def line_oriented(self):
        return True


class TestWarcIter(TestCase):

    def _warc_filepath(self, filename):
        return os.path.join(os.path.dirname(__file__), "warcs/{}".format(filename))

    def test_not_line_oriented(self):
        count = 0
        for count, (item_type, item) in enumerate(
                TestableNotLineOrientedWarcIter(
                    self._warc_filepath("test_1-20151202190229530-00000-29525-GLSS-F0G5RP-8000.warc.gz")), start=1):
            self.assertEqual("twitter_status", item_type)
            self.assertTrue(item.get("id"))
        self.assertEqual(1229, count)

    def test_item_type_limit(self):
        self.assertEqual(1229, len(list(TestableNotLineOrientedWarcIter(
            self._warc_filepath("test_1-20151202190229530-00000-29525-GLSS-F0G5RP-8000.warc.gz")).iter(
            ["twitter_status"]))))
        self.assertEqual(0, len(list(TestableNotLineOrientedWarcIter(
            self._warc_filepath("test_1-20151202190229530-00000-29525-GLSS-F0G5RP-8000.warc.gz")).iter(
            ["not_twitter_status"]))))

    def test_line_oriented_with_continuations(self):
        count = 0
        for count, (item_type, item) in enumerate(
                TestableLineOrientedWarcIter(
                    self._warc_filepath("test_1-20151202165907873-00000-306-60892de9dfc6-8001.warc.gz")), start=1):
            self.assertEqual("twitter_status", item_type)
            self.assertTrue(item.get("id"))
        self.assertEqual(1, count)

    def test_line_oriented_without_continuations(self):
        count = 0
        for count, (item_type, item) in enumerate(
                TestableLineOrientedWarcIter(
                    self._warc_filepath("test_1-20151202200525007-00000-30033-GLSS-F0G5RP-8000.warc.gz")), start=1):
            self.assertEqual("twitter_status", item_type)
            self.assertTrue(item.get("id"))
        self.assertEqual(111, count)

    def test_select_record(self):
        # Using a WARC that does not have records matching select_record.
        self.assertEqual(0, len(list(TestableNotLineOrientedWarcIter(
                    self._warc_filepath("test_1-20151202200525007-00000-30033-GLSS-F0G5RP-8000.warc.gz")).iter())))