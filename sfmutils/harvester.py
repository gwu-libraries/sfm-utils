from __future__ import absolute_import
import logging
import datetime
import pika
import json
import tempfile
import os
import shutil
import re
import hashlib
import codecs
import argparse
import sys
from collections import Counter, namedtuple
from sfmutils.state_store import JsonHarvestStateStore
from sfmutils.warcprox import warced
from sfmutils.utils import safe_string

log = logging.getLogger(__name__)

STATUS_SUCCESS = "completed success"
STATUS_FAILURE = "completed failure"
STATUS_RUNNING = "running"


class HarvestResult():
    """
    A data transfer object for keeping track of the results of a harvest.
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

    Where possible, code should be selected from harvest.CODE_*.
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

MqConfig = namedtuple("MqConfig", ["host", "username", "password", "exchange", "queue", "routing_keys"])

EXCHANGE = "sfm_exchange"


class BaseHarvester():
    def __init__(self, mq_config=None):
        self.mq_config = mq_config

        #Creating a connection can be skipped for testing purposes
        if mq_config:
            credentials = pika.PlainCredentials(mq_config.username, mq_config.password)
            parameters = pika.ConnectionParameters(host=mq_config.host, credentials=credentials)
            self._connection = pika.BlockingConnection(parameters)
            channel = self._connection.channel()
            #Declare sfm_exchange
            channel.exchange_declare(exchange=mq_config.exchange,
                                     type="topic", durable=True)
            #Declare harvester queue
            channel.queue_declare(queue=mq_config.queue,
                                  durable=True)
            #Bind
            for routing_key in mq_config.routing_keys:
                channel.queue_bind(exchange=mq_config.exchange,
                                   queue=mq_config.queue, routing_key=routing_key)

            channel.close()

    def consume(self):
        assert self.mq_config
        assert self._connection
        channel = self._connection.channel()
        channel.basic_qos(prefetch_count=1)
        log.info("Waiting for messages from %s", self.mq_config.queue)
        channel.basic_consume(self._callback, queue=self.mq_config.queue)
        channel.start_consuming()

    def _callback(self, channel, method, _, body):
        """
        Callback for receiving harvest message.

        Note that channel and method can be None to allow invoking from a
        non-mq environment.
        """
        log.info("Harvesting by message")

        #This way we can just use channel as a test
        if method is None:
            channel = None

        #Acknowledge the message
        if channel:
            log.debug("Acking message")
            channel.basic_ack(delivery_tag=method.delivery_tag)

        start_date = datetime.datetime.now()
        message = json.loads(body)
        log.debug("Message is %s" % json.dumps(message, indent=4))
        harvest_id = message["id"]
        collection_id = message["collection"]["id"]
        collection_path = message["collection"]["path"]

        prefix = safe_string(harvest_id)
        #Create a temp directory for WARCs
        warc_temp_dir = tempfile.mkdtemp(prefix=prefix)
        state_store = self.get_state_store(message)
        try:
            with warced(prefix, warc_temp_dir):
                harvest_result = self.harvest_seeds(message, state_store)
        except Exception as e:
            log.exception(e)
            harvest_result = HarvestResult()
            harvest_result.success = False
            harvest_result.errors.append(Msg(CODE_UNKNOWN_ERROR, str(e)))
        harvest_result.started = start_date
        harvest_result.ended = datetime.datetime.now()

        if harvest_result.success:
            #Send web harvest message
            self._send_web_harvest_message(channel, harvest_id, collection_id,
                                           collection_path, harvest_result.urls_as_set())
            #Process warc files
            for warc_filename in self._list_warcs(warc_temp_dir):
                #Move the warc
                dest_warc_filepath = self._move_file(warc_filename,
                                                     warc_temp_dir, self._path_for_warc(collection_path, warc_filename))
                harvest_result.warcs.append(dest_warc_filepath)
                #Send warc created message
                self._send_warc_created_message(channel, collection_id, collection_path, self._warc_id(warc_filename),
                                                dest_warc_filepath)

        #Delete temp dir
        if os.path.exists(warc_temp_dir):
            shutil.rmtree(warc_temp_dir)
        #Close state store
        state_store.close()
        self._send_status_message(channel, method.routing_key if method else "", harvest_id, harvest_result)
        return harvest_result

    def harvest_from_file(self, filepath):
        """
        Performs a harvest based on the a harvest start message contained in the
        provided filepath.

        No additional messages are sent, but state is updated.

        :param filepath: filepath of the harvest start message
        :return: the HarvestResult
        """
        log.debug("Harvesting from file %s", filepath)
        with codecs.open(filepath, "r") as f:
            body = f.read()
        return self._callback(None, None, None, body)

    def harvest_seeds(self, message, state_store):
        """
        Performs a harvest based on the seeds contained in the message.
        :param message: the message
        :param state_store: a state store
        :return: a HarvestResult
        """
        pass

    def get_state_store(self, message):
        """
        Gets a state store for the harvest.
        """
        return JsonHarvestStateStore(message["collection"]["path"])

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
            assert self.mq_config
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

    def _send_status_message(self, channel, harvest_routing_key, harvest_id, harvest_result):
        #Just add additional info to job message
        message = {
            "id": harvest_id,
            "status": STATUS_SUCCESS if harvest_result.success else STATUS_FAILURE,
            "infos": [msg.to_map() for msg in harvest_result.infos],
            "warnings": [msg.to_map() for msg in harvest_result.warnings],
            "errors": [msg.to_map() for msg in harvest_result.errors],
            "date_started": harvest_result.started.isoformat(),
            "date_ended": harvest_result.ended.isoformat(),
            "summary": harvest_result.summary,
            "token_updates": harvest_result.token_updates,
            "uids": harvest_result.uids
        }

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
        #Logging
        logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

        #Arguments
        parser = argparse.ArgumentParser()
        subparsers = parser.add_subparsers(dest="command")

        service_parser = subparsers.add_parser("service", help="Run harvesting service that consumes messages from "
                                                               "messaging queue.")
        service_parser.add_argument("host")
        service_parser.add_argument("username")
        service_parser.add_argument("password")

        seed_parser = subparsers.add_parser("seed", help="Harvest based on a seed file.")
        seed_parser.add_argument("filepath", help="Filepath of the seed file.")

        args = parser.parse_args()

        if args.command == "service":
            harvester = cls(mq_config=MqConfig(args.host, args.username, args.password, EXCHANGE, queue, routing_keys))
            harvester.consume()
        elif args.command == "seed":
            harvester = cls()
            result = harvester.harvest_from_file(args.filepath)
            if result:
                log.info("Result is: %s", result)
                sys.exit(0)
            else:
                log.warning("Result is: %s", result)
                sys.exit(1)
