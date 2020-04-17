from warcio.archiveiterator import WARCIterator
import json
import argparse
import logging
import sys
import os
from collections import namedtuple

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
        if isinstance(filepaths, str):
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
            with open(filepath, 'rb') as f:
                yield_count = 0
                for record_count, record in enumerate((r for r in WARCIterator(f) if r.rec_type == 'response')):
                    self._debug_counts(filename, record_count, yield_count, by_record_count=True)

                    record_url = record.rec_headers.get_header('WARC-Target-URI')
                    record_id = record.rec_headers.get_header('WARC-Record-ID')
                    if self._select_record(record_url):
                        stream = record.content_stream()
                        line = stream.readline().decode('utf-8')
                        while line:
                            json_obj = None
                            try:
                                if line != "\r\n":
                                    # A non-line-oriented payload only has one payload part.
                                    json_obj = json.loads(line)
                            except ValueError:
                                log.warning("Bad json in record %s: %s", record_id, line)
                            if json_obj:
                                for item_type, item_id, item_date, item in self._item_iter(record_url, json_obj):
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
                                                yield IterItem(item_type, item_id, item_date, record_url, item)
                                            else:
                                                log.warn("Bad response in record %s", record_id)
                            line = stream.readline().decode('utf-8')

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
        logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)

        main_limit_item_types = args.item_types.split(",") if "item_types" in vars(args) else None

        cls(args.filepaths).print_iter(limit_item_types=main_limit_item_types, pretty=args.pretty,
                                       print_item_type=args.print_item_type, dedupe=args.dedupe)
