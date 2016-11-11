from sfmutils.consumer import BaseConsumer, MqConfig, EXCHANGE
from sfmutils.api_client import ApiClient
from sfmutils.harvester import Msg, STATUS_SUCCESS, STATUS_FAILURE
import logging
import os
import json
from json.encoder import JSONEncoder
import datetime
import petl
from petl.util.base import dicts as _dicts
from petl.io.sources import write_source_from_arg
import codecs
import iso8601
import argparse
import sys
import shutil
import tempfile
from sfmutils.result import BaseResult, Msg, STATUS_SUCCESS, STATUS_FAILURE

log = logging.getLogger(__name__)

CODE_WARC_MISSING = "warc_missing"
CODE_NO_WARCS = "no_warcs"
CODE_UNSUPPORTED_EXPORT_FORMAT = "unsupported_export_format"
CODE_BAD_REQUEST = "bad_request"


class ExportResult(BaseResult):
    """
    Keeps track of the results of an export.
    """

    def __init__(self):
        BaseResult.__init__(self)

    def _result_name(self):
        return "Export"


class BaseExporter(BaseConsumer):
    def __init__(self, api_base_url, warc_iter_cls, table_cls, working_path, mq_config=None, warc_base_path=None,
                 limit_item_types=None):
        BaseConsumer.__init__(self, mq_config=mq_config, working_path=working_path, persist_messages=True)
        self.api_client = ApiClient(api_base_url)
        self.warc_iter_cls = warc_iter_cls
        self.table_cls = table_cls
        self.limit_item_types = limit_item_types
        # This is for unit tests only.
        self.warc_base_path = warc_base_path

    def on_message(self):
        assert self.message

        export_id = self.message["id"]
        log.info("Performing export %s", export_id)

        self.result = ExportResult()
        self.result.started = datetime.datetime.now()

        # Get the WARCs from the API
        collection_id = self.message.get("collection", {}).get("id")
        seed_ids = []
        seed_uids = []
        for seed in self.message.get("seeds", []):
            seed_ids.append(seed["id"])
            seed_uids.append(seed["uid"])

        if (collection_id or seed_ids) and not (collection_id and seed_ids):
            harvest_date_start = self.message.get("harvest_date_start")
            harvest_date_end = self.message.get("harvest_date_end")
            warc_paths = self._get_warc_paths(collection_id, seed_ids, harvest_date_start, harvest_date_end)
            export_format = self.message["format"]
            export_path = self.message["path"]
            dedupe = self.message.get("dedupe", False)
            item_date_start = iso8601.parse_date(
                self.message["item_date_start"]) if "item_date_start" in self.message else None
            item_date_end = iso8601.parse_date(
                self.message["item_date_end"]) if "item_date_end" in self.message else None
            temp_path = os.path.join(self.working_path, "tmp")
            base_filepath = os.path.join(temp_path, export_id)

            if warc_paths:

                # Clean the temp directory
                if os.path.exists(temp_path):
                    shutil.rmtree(temp_path)
                os.makedirs(temp_path)

                # We get a lot of bang from PETL
                export_formats = {
                    "csv": ("csv", petl.tocsv),
                    "tsv": ("tsv", petl.totsv),
                    "html": ("html", petl.tohtml),
                    "xlsx": ("xlsx", petl.toxlsx),
                    "json": ("json", to_lineoriented_json)
                }
                # Other possibilities: XML, databases, HDFS
                if export_format == "json_full":
                    filepath = "{}.json".format(base_filepath)
                    log.info("Exporting to %s", filepath)
                    self._full_json_export(warc_paths, filepath, dedupe, item_date_start, item_date_end, seed_uids)
                elif export_format == "dehydrate":
                    table = self.table_cls(warc_paths, dedupe, item_date_start, item_date_end, seed_uids)
                    filepath = "{}.txt".format(base_filepath)
                    log.info("Exporting to %s", filepath)
                    petl.totext(table, filepath, template="{{{}}}\n".format(table.id_field()))
                elif export_format in export_formats:
                    table = self.table_cls(warc_paths, dedupe, item_date_start, item_date_end, seed_uids)
                    filepath = "{}.{}".format(base_filepath, export_formats[export_format][0])
                    log.info("Exporting to %s", filepath)
                    export_formats[export_format][1](table, filepath)
                    if export_format == 'html':
                        self._file_fix(filepath, prefix="<html><head><meta charset='utf-8'></head>\n", suffix="</html>")
                else:
                    self.result.errors.append(
                        Msg(CODE_UNSUPPORTED_EXPORT_FORMAT, "{} is not supported".format(export_format)))
                    self.result.success = False

                # Move files from temp path to export path
                if os.path.exists(export_path):
                    shutil.rmtree(export_path)
                shutil.move(temp_path, export_path)

            else:
                self.result.errors.append(Msg(CODE_NO_WARCS, "No WARC files from which to export"))
                self.result.success = False

        else:
            self.result.errors.append(Msg(CODE_BAD_REQUEST, "Request export of a seed or collection."))
            self.result.success = False

        self.result.ended = datetime.datetime.now()
        self._send_response_message(self.routing_key, export_id, self.result)

    @staticmethod
    def _file_fix(filepath, prefix=None, suffix=None):
        """
        create a temp file to save the large file object, don't
        need to load file to memory
        """
        with tempfile.NamedTemporaryFile(dir='.', delete=False) as outfile:
            if prefix:
                outfile.write(prefix)
            shutil.copyfileobj(file(filepath, 'r'), outfile)
            if suffix:
                outfile.write(suffix)
        shutil.move(outfile.name, filepath)

    def _full_json_export(self, warc_paths, export_filepath, dedupe, item_date_start, item_date_end, seed_uids):
        with codecs.open(export_filepath, "w") as f:
            for status in self.warc_iter_cls(warc_paths, seed_uids).iter(dedupe=dedupe,
                                                                         item_date_start=item_date_start,
                                                                         item_date_end=item_date_end,
                                                                         limit_item_types=self.limit_item_types):
                json.dump(status.item, f)
                f.write("\n")

    def _get_warc_paths(self, collection_id, seed_ids, harvest_date_start, harvest_date_end):
        """
        Get list of WARC files and make sure they exists.
        """
        warc_paths = []
        log.debug("Getting warcs for collection %s", collection_id)
        for warc in self.api_client.warcs(collection_id=collection_id, seed_ids=seed_ids,
                                          harvest_date_start=harvest_date_start, harvest_date_end=harvest_date_end,
                                          exclude_web=True):
            warc_path = os.path.join(self.warc_base_path, warc["path"]) if self.warc_base_path else warc["path"]
            if os.path.exists(warc_path):
                warc_paths.append(warc_path)
            else:
                self.result.errors.append(Msg(CODE_WARC_MISSING, "{} is missing".format(warc_path)))
                self.result.success = False
        log.debug("Warcs are %s", warc_paths)
        return warc_paths

    def _send_response_message(self, export_request_routing_key, export_id, export_result):
        # Just add additional info to job message
        message = {
            "id": export_id,
            "status": STATUS_SUCCESS if export_result.success else STATUS_FAILURE,
            "infos": [msg.to_map() for msg in export_result.infos],
            "warnings": [msg.to_map() for msg in export_result.warnings],
            "errors": [msg.to_map() for msg in export_result.errors],
            "date_started": export_result.started.isoformat(),
        }

        if export_result.ended:
            message["date_ended"] = export_result.ended.isoformat()

        # Routing key may be none
        response_routing_key = export_request_routing_key.replace("start", "status")
        self._publish_message(response_routing_key, message)

    @staticmethod
    def main(cls, queue, routing_keys):
        """
        A configurable main() for an exporter.

        For example:
            if __name__ == "__main__":
                FlickrExporter.main(FlickrExporter, QUEUE, [ROUTING_KEY])

        :param cls: the exporter class
        :param queue: queue for the harvester
        :param routing_keys: list of routing keys for the exporter
        """

        # Logging
        logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

        # Arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--debug", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                            default="False", const="True")

        subparsers = parser.add_subparsers(dest="command")

        service_parser = subparsers.add_parser("service", help="Run export service that consumes messages from "
                                                               "messaging queue.")
        service_parser.add_argument("host")
        service_parser.add_argument("username")
        service_parser.add_argument("password")
        service_parser.add_argument("api")
        service_parser.add_argument("working_path")
        service_parser.add_argument("--skip-resume", action="store_true")

        file_parser = subparsers.add_parser("file", help="Export based on a file.")
        file_parser.add_argument("filepath", help="Filepath of the export file.")
        file_parser.add_argument("api", help="Base url of SFM-UI API")
        file_parser.add_argument("working_path")
        file_parser.add_argument("--host")
        file_parser.add_argument("--username")
        file_parser.add_argument("--password")

        args = parser.parse_args()

        # Logging
        logging.getLogger().setLevel(logging.DEBUG if args.debug else logging.INFO)

        if args.command == "service":
            exporter = cls(args.api, args.working_path,
                           mq_config=MqConfig(args.host, args.username, args.password, EXCHANGE,
                                              {queue: routing_keys}))
            if not args.skip_resume:
                exporter.resume_from_file()
            exporter.run()
        elif args.command == "file":
            mq_config = MqConfig(args.host, args.username, args.password, EXCHANGE, None) \
                if args.host and args.username and args.password else None
            exporter = cls(args.api, args.working_path, mq_config=mq_config)
            exporter.message_from_file(args.filepath)
            if exporter.result:
                log.info("Result is: %s", exporter.result)
                sys.exit(0)
            else:
                log.warning("Result is: %s", exporter.result)
                sys.exit(1)


class BaseTable(petl.Table):
    """
    A base PETL Table.
    """

    def __init__(self, warc_paths, dedupe, item_date_start, item_date_end, seed_uids, warc_iter_cls,
                 limit_item_types=None):
        self.warc_paths = warc_paths
        self.dedupe = dedupe
        self.item_date_start = item_date_start
        self.item_date_end = item_date_end
        self.seed_uids = seed_uids
        self.warc_iter_cls = warc_iter_cls
        self.limit_item_types = limit_item_types

    def _header_row(self):
        """
        Returns a tuple of header labels.

        For example, ("photo_id", "date_posted", "license")
        """
        return ()

    def _row(self, item):
        """
        Returns a tuple of values for an item.
        """
        return ()

    def id_field(self):
        """
        Name of the field containing the identifier. This should watch one of the header labels.
        """
        pass

    def __iter__(self):
        # yield the header row
        yield self._header_row()
        for post in self.warc_iter_cls(self.warc_paths,
                                       self.seed_uids).iter(dedupe=self.dedupe,
                                                            item_date_start=self.item_date_start,
                                                            item_date_end=self.item_date_end,
                                                            limit_item_types=self.limit_item_types):
            try:
                yield self._row(post.item)
            except KeyError, e:
                log.warn("Invalid key %s in %s", e.message, json.dumps(post.item, indent=4))


class DateEncoder(JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        # Let the base class default method deal with others
        return JSONEncoder.default(self, obj)


def to_lineoriented_json(table, source):
    """
    Function to enabling PETL support for exporting line-oriented JSON.
    """
    source = write_source_from_arg(source)
    encoder = DateEncoder()
    with source.open("wb") as f:
        for d in _dicts(table):
            for chunk in encoder.iterencode(d):
                f.write(chunk)
            f.write("\n")
