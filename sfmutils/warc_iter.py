from __future__ import absolute_import
import sfmutils.warc as warc
import json
import argparse
import logging
import sys
import os
from collections import namedtuple
from urllib3.exceptions import ProtocolError

log = logging.getLogger(__name__)

IterItem = namedtuple('IterItem', ['type', 'id', 'date', 'url', 'item'])


class BaseWarcIter:
    """
    Base class for a warc iterator. A warc iterator iterates over the social media
    items recorded in a WARC file.

    This supports payloads which are json or line-oriented json.

    Subclasses should overrride _select_record(), _item_iter(), item_types, and
    possibly line_oriented.
    """

    def __init__(self, filepaths):
        if isinstance(filepaths, basestring):
            self.filepaths = (filepaths,)
        else:
            self.filepaths = filepaths

    def __iter__(self):
        return self.iter()

    @staticmethod
    def _debug_counts(filename, record_count, yield_count, by_record_count=True):
        should_debug = False
        if by_record_count and record_count <= 100 and record_count % 10 == 0:
            should_debug = True
        elif by_record_count and 100 < record_count and record_count % 100 == 0:
            should_debug = True
        elif not by_record_count and yield_count <= 1000 and yield_count % 100 == 0:
            should_debug = True
        elif not by_record_count and 1000 < yield_count and yield_count % 1000 == 0:
            should_debug = True
        if should_debug:
            log.debug("File %s. Processed %s records. Yielded %s items.", filename, record_count, yield_count)

    def iter(self, limit_item_types=None, dedupe=False, item_date_start=None, item_date_end=None):
        """
        :return: Iterator returning IterItems.
        """
        seen_ids = {}
        for filepath in self.filepaths:
            log.info("Iterating over %s", filepath)
            filename = os.path.basename(filepath)
            f = warc.WARCResponseFile(filepath)
            yield_count = 0
            for record_count, record in enumerate(f):
                self._debug_counts(filename, record_count, yield_count, by_record_count=True)

                if self._select_record(record.url):
                    # An iterator over json objects which constitute the payload of a record.
                    if not self.line_oriented:
                        # A non-line-oriented payload only has one payload part.
                        payload_data = ""
                        # Handles chunk encoding
                        encoding_type = record.http_response.getheader('transfer-encoding')
                        if encoding_type and encoding_type.lower() == "chunked":
                            for line in record.http_response.read_chunked(decode_content=True):
                                payload_data += line
                        else:
                            payload_data = record.http_response.data
                        payload_parts_iter = [payload_data]
                    else:
                        # A line-oriented payload has many payload parts.
                        payload_parts_iter = self._iter_lines(record.http_response)
                    for payload_part in payload_parts_iter:
                        json_obj = None
                        try:
                            # A non-line-oriented payload only has one payload part.
                            json_obj = json.loads(payload_part)
                        except ValueError:
                            log.warn("Bad json in record %s: %s", record.header.record_id, payload_part)
                        if json_obj:
                            for item_type, item_id, item_date, item in self._item_iter(record.url, json_obj):
                                # None for item_type indicates that the type is not handled. OK to ignore.
                                if item_type is not None:
                                    yield_item = True
                                    if limit_item_types and item_type not in limit_item_types:
                                        yield_item = False
                                    if item_date_start and item_date and item_date < item_date_start:
                                        yield_item = False
                                    if item_date_end and item_date and item_date > item_date_end:
                                        yield_item = False
                                    if not self._select_item(item):
                                        yield_item = False
                                    if dedupe and yield_item:
                                        if item_id in seen_ids:
                                            yield_item = False
                                        else:
                                            seen_ids[item_id] = True
                                    if yield_item:
                                        if item is not None:
                                            yield_count += 1
                                            self._debug_counts(filename, record_count, yield_count,
                                                               by_record_count=False)
                                            yield IterItem(item_type, item_id, item_date, record.url, item)
                                        else:
                                            log.warn("Bad response in record %s", record.header.record_id)

    def _select_record(self, url):
        """
        Return True to process this record. This allows a WarcIter to only process
        records for the type of social media content that it handles.
        """
        pass

    def _select_item(self, item):
        """
        Return True to select this item. This allows a WarcIter to filter items.
        """
        return True

    def print_iter(self, pretty=False, fp=sys.stdout, limit_item_types=None, print_item_type=False, dedupe=False):
        for item_type, _, _, _, item in self.iter(limit_item_types=limit_item_types, dedupe=dedupe):
            if print_item_type:
                fp.write("{}:".format(item_type))
            json.dump(item, fp, indent=4 if pretty else None)
            fp.write("\n")

    def _item_iter(self, url, json_obj):
        """
        Returns an iterator over the social media item types and items (as JSON objects).
        :returns item_type, item_id, item_date, item iterator
        """
        pass

    @staticmethod
    def _iter_lines(http_response):
        """
        Iterates over the response data, one line at a time.

        Borrowed from https://github.com/kennethreitz/requests/blob/master/requests/models.py.
        """
        try:
            pending = None

            for chunk in http_response.stream(decode_content=True):

                if pending is not None:
                    chunk = pending + chunk

                lines = chunk.splitlines()

                if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                    pending = lines.pop()
                else:
                    pending = None

                for line in lines:
                    yield line

            if pending is not None:
                yield pending
        except ProtocolError:
            # Last chunk incomplete
            pass

    @staticmethod
    def item_types():
        """
        Returns a list of item types that are handled by this WarcIter.
        """
        pass

    @property
    def line_oriented(self):
        """
        Indicates whether the payload should be handled as line-oriented.

        Subclasses that support line-oriented payloads should return True.
        """
        return False

    @staticmethod
    def main(cls):
        # Logging
        logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

        parser = argparse.ArgumentParser()
        item_types = cls.item_types()
        if len(item_types) > 1:
            parser.add_argument("--item-types",
                                help="A comma separated list of item types to limit the results. "
                                     "Item types are {}".format(", ".join(item_types)))
        parser.add_argument("--pretty", action="store_true", help="Format the json for viewing.")
        parser.add_argument("--dedupe", action="store_true", help="Remove duplicate items.")
        parser.add_argument("--print-item-type", action="store_true", help="Print the item type.")
        parser.add_argument("--debug", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                            default="False", const="True")
        parser.add_argument("filepaths", nargs="+", help="Filepath of the warc.")

        args = parser.parse_args()

        # Logging
        logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s',
                            level=logging.DEBUG if args.debug else logging.INFO)

        main_limit_item_types = args.item_types.split(",") if "item_types" in vars(args) else None

        cls(args.filepaths).print_iter(limit_item_types=main_limit_item_types, pretty=args.pretty,
                                       print_item_type=args.print_item_type, dedupe=args.dedupe)
