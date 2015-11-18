from __future__ import absolute_import
import logging
import datetime
import json
import tempfile
import shutil
import hashlib
import argparse
import sys
import threading
import signal
from collections import Counter

import pika
import os
import re
import codecs
from sfmutils.consumer import BaseConsumer, MqConfig, EXCHANGE
from sfmutils.state_store import JsonHarvestStateStore
from sfmutils.warcprox import warced
from sfmutils.utils import safe_string


log = logging.getLogger(__name__)

STATUS_SUCCESS = "completed success"
STATUS_FAILURE = "completed failure"
STATUS_RUNNING = "running"


class HarvestResult():
    """
    Keeps track of the results of a harvest.
    """
    def __init__(self):
        self.success = True
        self.started = None
        self.ended = None
        self.infos = []
        self.warnings = []
        self.errors = []
        self.urls = []
        self.warcs = []
        self.summary = Counter()
        #Map of uids to tokens for which tokens have been found to have changed.
        self.token_updates = {}
        #Map of tokens to uids for tokens for which uids have been found.
        self.uids = {}

    def __nonzero__(self):
        return 1 if self.success else 0

    def __str__(self):
        harv_str = "Harvest response is {}.".format(self.success)
        if self.started:
            harv_str += " Started: {}".format(self.started)
        if self.ended:
            harv_str += " Ended: {}".format(self.ended)
        harv_str += self._str_messages(self.infos, "Informational")
        harv_str += self._str_messages(self.warnings, "Warning")
        harv_str += self._str_messages(self.errors, "Error")
        if self.warcs:
            harv_str += " Warcs: {}".format(self.warcs)
        if self.urls:
            harv_str += " Urls: {}".format(self.urls)
        if self.summary:
            harv_str += " Harvest summary: {}".format(self.summary)
        if self.token_updates:
            harv_str += " Token updates: {}".format(self.token_updates)
        if self.uids:
            harv_str += " Uids: {}".format(self.uids)
        return harv_str

    @staticmethod
    def _str_messages(messages, name):
        msg_str = ""
        if messages:
            msg_str += " {} messages are:".format(name)

        for (i, msg) in enumerate(messages, start=1):
            msg_str += "({}) [{}] {}".format(i, msg["code"], msg["message"])

        return msg_str

    def merge(self, other):
        self.success = self.success and other.success
        self.started = self.started or other.started
        self.ended = self.ended or other.ended
        self.infos.extend(other.infos)
        self.warnings.extend(other.warnings)
        self.errors.extend(other.errors)
        self.warcs.extend(other.warcs)
        self.urls.extend(other.urls)
        self.summary.update(other.summary)

    def urls_as_set(self):
        return set(self.urls)

    def increment_summary(self, key, increment=1):
        self.summary[key] += increment

CODE_UNKNOWN_ERROR = "unknown_error"


class Msg():
    """
    An informational, warning, or error message to be included in the harvest
    status.

    Where possible, code should be selected from harvester.CODE_*.
    """
    def __init__(self, code, message):
        assert code
        assert message
        self.code = code
        self.message = message

    def to_map(self):
        return {
            "code": self.code,
            "message": self.message
        }


class BaseHarvester(BaseConsumer):
    """
    Base class for a harvester, allowing harvesting from a queue or from a file.

    Note that streams should only be harvested from a file as this does not support
    harvest stop messages. (See sfm-utils.stream_consumer.StreamConsumer.)

    Subclasses should overrride harvest_seeds().
    """
    def __init__(self, mq_config, process_interval_secs=1200):
        BaseConsumer.__init__(self, mq_config)
        self.process_interval_secs = process_interval_secs

        self.is_streaming = False
        self.harvest_result = None
        self.harvest_result_lock = None
        self.message_body = None
        self.message = None
        self.channel = None
        self.routing_key = ""
        self.warc_temp_dir = None
        self.stop_event = None
        self.process_timer = None
        self.state_store = None

    def on_message(self):
        assert self.message_body

        log.info("Harvesting by message")

        self.harvest_result = HarvestResult()
        self.harvest_result.started = datetime.datetime.now()
        self.harvest_result_lock = threading.Lock()
        self.stop_event = threading.Event()

        def set_stop_event(signal_number, stack_frame):
            self.stop_event.set()

        signal.signal(signal.SIGTERM, set_stop_event)
        signal.signal(signal.SIGINT, set_stop_event)

        self.message = json.loads(self.message_body)
        log.debug("Message is %s" % json.dumps(self.message, indent=4))

        prefix = safe_string(self.message["id"])
        #Create a temp directory for WARCs
        self.warc_temp_dir = tempfile.mkdtemp(prefix=prefix)
        self._create_state_store()

        #Setup the process timer
        def process_it(h):
            h._process(done=False)
            h.process_timer = threading.Timer(h.process_interval_secs, process_it, args=[h])
            h.process_timer.start()
        if self.is_streaming:
            self.process_timer = threading.Timer(self.process_interval_secs, process_it, args=[self])
            self.process_timer.start()

        try:
            with warced(prefix, self.warc_temp_dir):
                self.harvest_seeds()
        except Exception as e:
            log.exception("Unknown error raised during harvest")
            self.harvest_result.success = False
            self.harvest_result.errors.append(Msg(CODE_UNKNOWN_ERROR, str(e)))
        self.harvest_result.ended = datetime.datetime.now()
        if self.process_timer:
            self.process_timer.cancel()

        self._process()

        #Delete temp dir
        if os.path.exists(self.warc_temp_dir):
            shutil.rmtree(self.warc_temp_dir)

    def _process(self, done=True):
        harvest_id = self.message["id"]
        collection_id = self. message["collection"]["id"]
        collection_path = self.message["collection"]["path"]

        #Acquire a lock
        with self.harvest_result_lock:
            if self.harvest_result.success:
                #Send web harvest message
                self._send_web_harvest_message(self.channel, harvest_id, collection_id,
                                               collection_path, self.harvest_result.urls_as_set())
                #Since the urls were sent, clear them
                if not done:
                    self.harvest_result.urls = []

                #Process warc files
                for warc_filename in self._list_warcs(self.warc_temp_dir):
                    #Move the warc
                    dest_warc_filepath = self._move_file(warc_filename,
                                                         self.warc_temp_dir,
                                                         self._path_for_warc(collection_path, warc_filename))
                    self.harvest_result.warcs.append(dest_warc_filepath)
                    #Send warc created message
                    self._send_warc_created_message(self.channel, collection_id, collection_path,
                                                    self._warc_id(warc_filename), dest_warc_filepath)

            #TODO: Persist summary so that can resume

            status = STATUS_SUCCESS if self.harvest_result.success else STATUS_FAILURE
            if not self.harvest_result.success:
                status = STATUS_FAILURE
            elif not done:
                status = STATUS_RUNNING
            else:
                status = STATUS_SUCCESS
            self._send_status_message(self.channel, self.routing_key, harvest_id,
                                  self.harvest_result, status)
            if not done:
                #Since these were sent, clear them.
                self.harvest_result.errors = []
                self.harvest_result.infos = []
                self.harvest_result.warnings = []
                self.harvest_result.token_updates = []
                self.harvest_result.uids = []

    def harvest_from_file(self, filepath, routing_key=None, is_streaming=False):
        """
        Performs a harvest based on the a harvest start message contained in the
        provided filepath.

        SIGTERM or SIGINT (Ctrl+C) will interrupt.

        :param filepath: filepath of the harvest start message
        :param routing_key: routing key of the harvest start message
        :param is_streaming: True to run in streaming mode
        """
        log.debug("Harvesting from file %s", filepath)
        with codecs.open(filepath, "r") as f:
            self.message_body = f.read()

        self.routing_key = routing_key or ""
        self.is_streaming = is_streaming

        if self._connection:
            self.channel = self._connection.channel()
        self.on_message()
        if self.channel:
            self.channel.close()
        return self.harvest_result

    def harvest_seeds(self):
        """
        Performs a harvest based on the seeds contained in the message.

        When called, self.message_body, self.routing_key, self.channel,
        and self.is_streaming will be populated.
        """
        pass

    def _create_state_store(self):
        """
        Creates a state store for the harvest.
        """
        self.state_store = JsonHarvestStateStore(self.message["collection"]["path"])

    @staticmethod
    def _list_warcs(path):
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and
                (f.endswith(".warc") or f.endswith(".warc.gz"))]

    @staticmethod
    def _path_for_warc(collection_path, filename):
        m = re.search("-(\d{4})(\d{2})(\d{2})(\d{2})\d{7}-", filename)
        assert m
        return "/".join([collection_path, m.group(1), m.group(2), m.group(3), m.group(4)])

    @staticmethod
    def _warc_id(warc_filename):
        return warc_filename.replace(".warc", "").replace(".gz", "")

    @staticmethod
    def _move_file(filename, src_path, dest_path):
        src_filepath = os.path.join(src_path, filename)
        dest_filepath = os.path.join(dest_path, filename)
        log.debug("Moving %s to %s", src_filepath, dest_filepath)
        if not os.path.exists(dest_path):
            os.makedirs(dest_path)
        shutil.move(src_filepath, dest_filepath)
        return dest_filepath

    def _publish_message(self, routing_key, message, channel):
        message_body = json.dumps(message, indent=4)
        if channel:
            log.debug("Sending message to sfm_exchange with routing_key %s. The body is: %s", routing_key, message_body)
            channel.basic_publish(exchange=self.mq_config.exchange,
                                  routing_key=routing_key,
                                  properties=pika.BasicProperties(content_type="application/json",
                                                                  delivery_mode=2),
                                  body=message_body)
        else:
            log.debug("Skipping sending message to sfm_exchange with routing_key %s. The body is: %s",
                      routing_key, message_body)

    def _send_web_harvest_message(self, channel, harvest_id, collection_id, collection_path, urls):
        message = {
            #TODO: Make this unique when multiple web harvest messages are sent.
            #This will be unique
            "id": "{}:{}".format(self.__class__.__name__, harvest_id),
            "parent_id": harvest_id,
            "type": "web",
            "seeds": [],
            "collection": {
                "id": collection_id,
                "path": collection_path
            }
        }
        for url in urls:
            message["seeds"].append({"token": url})

        self._publish_message("harvest.start.web", message, channel)

    def _send_status_message(self, channel, harvest_routing_key, harvest_id, harvest_result, status):
        #Just add additional info to job message
        message = {
            "id": harvest_id,
            "status": status,
            "infos": [msg.to_map() for msg in harvest_result.infos],
            "warnings": [msg.to_map() for msg in harvest_result.warnings],
            "errors": [msg.to_map() for msg in harvest_result.errors],
            "date_started": harvest_result.started.isoformat(),
            "summary": harvest_result.summary,
            "token_updates": harvest_result.token_updates,
            "uids": harvest_result.uids
        }
        if harvest_result.ended:
            message["date_ended"] = harvest_result.ended.isoformat(),

        #Routing key may be none
        status_routing_key = harvest_routing_key.replace("start", "status")
        self._publish_message(status_routing_key, message, channel)

    def _send_warc_created_message(self, channel, collection_id, collection_path, warc_id, warc_path):
        message = {
            "collection": {
                "id": collection_id,
                "path": collection_path

            },
            "warc": {
                "id": warc_id,
                "path": warc_path,
                "date_created": datetime.datetime.fromtimestamp(os.path.getctime(warc_path)).isoformat(),
                "bytes": os.path.getsize(warc_path),
                "sha1": hashlib.sha1(open(warc_path).read()).hexdigest()
            }
        }
        self._publish_message("warc_created", message, channel)

    @staticmethod
    def main(cls, queue, routing_keys):
        """
        A configurable main() for a harvester.

        For example:
            if __name__ == "__main__":
                TwitterHarvester.main(TwitterHarvester, QUEUE, [ROUTING_KEY])

        :param cls: the harvester class
        :param queue: queue for the harvester
        :param routing_keys: list of routing keys for the harvester
        """

        #Logging
        logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

        #Arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--debug", action="store_true")
        parser.add_argument("--debug-pika", action="store_true")

        subparsers = parser.add_subparsers(dest="command")

        service_parser = subparsers.add_parser("service", help="Run harvesting service that consumes messages from "
                                                               "messaging queue.")
        service_parser.add_argument("host")
        service_parser.add_argument("username")
        service_parser.add_argument("password")

        seed_parser = subparsers.add_parser("seed", help="Harvest based on a seed file.")
        seed_parser.add_argument("filepath", help="Filepath of the seed file.")
        seed_parser.add_argument("--streaming", action="store_true", help="Run in streaming mode.")
        seed_parser.add_argument("--host")
        seed_parser.add_argument("--username")
        seed_parser.add_argument("--password")
        seed_parser.add_argument("--routing-key")

        args = parser.parse_args()

        #Logging
        logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s',
                            level=logging.DEBUG if args.debug or args.debug_pika else logging.INFO)
        logging.getLogger("pika").setLevel(logging.debug if args.debug_pika else logging.INFO)

        if args.command == "service":
            harvester = cls(MqConfig(args.host, args.username, args.password, EXCHANGE,
                                               {queue: routing_keys}))
            harvester.consume()
        elif args.command == "seed":
            harvester = cls(MqConfig(args.host, args.username, args.password, None, None, None)
                            if args.routing_key else None)
            harvester.harvest_from_file(args.filepath, routing_key=args.routing_key, is_streaming=args.streaming)
            if harvester.harvest_result:
                log.info("Result is: %s", harvester.harvest_result)
                sys.exit(0)
            else:
                log.warning("Result is: %s", harvester.harvest_result)
                sys.exit(1)
