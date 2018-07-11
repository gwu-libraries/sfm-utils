import logging
import json
import shutil
import hashlib
import argparse
import sys
import threading
import signal
from collections import Counter, OrderedDict
import os
import re
import codecs
import uuid
import iso8601
from datetime import date
from queue import Queue, Empty

from sfmutils.consumer import BaseConsumer, MqConfig, EXCHANGE
from sfmutils.state_store import JsonHarvestStateStore, DelayedSetStateStoreAdapter
from sfmutils.warcprox import warced
from sfmutils.utils import safe_string, datetime_from_stamp, datetime_now
from sfmutils.result import BaseResult, Msg, STATUS_SUCCESS, STATUS_FAILURE, STATUS_RUNNING, STATUS_PAUSED, \
    STATUS_STOPPING

log = logging.getLogger(__name__)


class HarvestResult(BaseResult):
    """
    Keeps track of the results of a harvest.
    """

    def __init__(self):
        BaseResult.__init__(self)
        self.warcs = []
        self.warc_bytes = 0
        # Map of days to counters (map of items to counts)
        self._stats = OrderedDict()
        # Map of uids to tokens for which tokens have been found to have changed.
        self.token_updates = {}
        # Map of tokens to uids for tokens for which uids have been found.
        self.uids = {}
        # A counter of harvested items for testing purposes.
        self.harvest_counter = Counter()

    def _result_name(self):
        return "Harvest"

    def _addl_str(self):
        harv_str = ""
        if self.warcs:
            harv_str += " Warcs: {}".format(self.warcs)
        if self.warc_bytes:
            harv_str += " Warc bytes: {}".format(self.warc_bytes)
        if self._stats:
            harv_str += " Harvest stats: {}".format(self.stats_summary())
        if self.token_updates:
            harv_str += " Token updates: {}".format(self.token_updates)
        if self.uids:
            harv_str += " Uids: {}".format(self.uids)
        return harv_str

    def increment_stats(self, item, count=1, day=None):
        if day is None:
            day = date.today()
        if day not in self._stats:
            self._stats[day] = Counter()
        self._stats[day][item] += count

    def stats(self):
        """
        Returns ordered dictinary of day to stats.
        """
        return self._stats

    def stats_summary(self):
        """
        Returns counter that aggregates the stats.
        """
        summary = Counter()
        for stats in self._stats.values():
            summary.update(stats)
        return summary

    def add_warc(self, filepath):
        self.warcs.append(filepath)
        self.warc_bytes += os.path.getsize(filepath)


# Any exception thrown by the harvester.
CODE_UNKNOWN_ERROR = "unknown_error"
# Token not recognized by API.
CODE_TOKEN_NOT_FOUND = "token_not_found"
# UID not recognized by API.
CODE_UID_NOT_FOUND = "uid_not_found"
# Token is unauthorized/private.
CODE_TOKEN_UNAUTHORIZED = "token_unauthorized"
# UID is unauthorized/private.
CODE_UID_UNAUTHORIZED = "uid_unauthorized"
# Token is suspended.
CODE_TOKEN_SUSPENDED = "token_suspended"
# UID is suspended.
CODE_UID_SUSPENDED = "uid_suspended"
# A resume occurred
CODE_HARVEST_RESUMED = "harvest_resumed"
# Error during persisting message
CODE_MSG_PERSIST_ERROR = "msg_persist_error"


class BaseHarvester(BaseConsumer):
    """
    Base class for a harvester, allowing harvesting from a queue or from a file.

    Note that streams should only be harvested from a file as this does not support
    harvest stop messages. (See sfm-utils.stream_consumer.StreamConsumer.)

    Subclasses should overrride harvest_seeds().
    """

    def __init__(self, working_path, mq_config=None, stream_restart_interval_secs=30 * 60, debug=False,
                 use_warcprox=True, queue_warc_files_interval_secs=5 * 60, warc_rollover_secs=30 * 60,
                 debug_warcprox=False, tries=3, host=None):
        BaseConsumer.__init__(self, working_path=working_path, mq_config=mq_config, persist_messages=True)
        self.stream_restart_interval_secs = stream_restart_interval_secs
        self.is_streaming = False
        self.routing_key = ""
        self.warc_temp_dir = None
        self.stop_harvest_seeds_event = threading.Event()
        self.stop_harvest_loop_event = threading.Event()
        self.restart_stream_timer = None
        self.state_store = None
        self.debug = debug
        self.debug_warcprox = debug_warcprox
        self.use_warcprox = use_warcprox
        self.warc_processing_queue = Queue()
        self.result_filepath = None
        self.queue_warc_files_interval_secs = queue_warc_files_interval_secs
        self.queue_warc_files_timer = None
        self.warc_rollover_secs = warc_rollover_secs
        self.tries = tries

        # Create and start warc processing thread.
        self.warc_processing_thread = threading.Thread(target=self._process_warc_thread, name="warc_processing_thread")
        self.warc_processing_thread.daemon = True
        self.warc_processing_thread.start()
        self.host = host or os.environ.get("HOSTNAME", "localhost")
        # Indicates that the next shutdown should be treated as a pause of the harvest, rather than a completion.
        self.is_pause = False

    def on_message(self):
        assert self.message

        log.info("Harvesting by message with id %s", self.message["id"])

        self.result_filepath = os.path.join(self.working_path, "{}_result.json".format(safe_string(self.message["id"])))

        # Create a temp directory for WARCs
        self.warc_temp_dir = self._create_warc_temp_dir()
        self._create_state_store()

        # Possibly resume a harvest
        self.result = HarvestResult()
        self.result.started = datetime_now()

        if os.path.exists(self.result_filepath) or len(self._list_warcs(self.warc_temp_dir)) > 0:
            self._load_result()
            self.result.warnings.append(
                Msg(CODE_HARVEST_RESUMED, "Harvest resumed on {}".format(datetime_now())))
            # Send a status message. This will give immediate indication that harvesting is occurring.
            self._send_status_message(STATUS_RUNNING)
            self._queue_warc_files()
        else:
            # Send a status message. This will give immediate indication that harvesting is occurring.
            self._send_status_message(STATUS_RUNNING)

        # stop_harvest_loop_event tells the harvester to stop looping.
        # Only streaming harvesters loop.
        # For other harvesters, this is tripped after the first entrance into loop.
        self.stop_harvest_loop_event = threading.Event()

        # Supervisor sends a signal, indicating that the harvester should stop.
        # This is a graceful shutdown. Harvesting seeds is stopped and processing
        # is finished. This may take some time.
        def shutdown(signal_number, stack_frame):
            log.info("Shutdown triggered")
            # This is for the consumer.
            self.should_stop = True
            if self.is_pause:
                log.info("This will be a pause of the harvest.")
            self.stop_harvest_loop_event.set()
            # stop_event tells the harvester to stop harvest_seeds.
            # This will allow warcprox to exit.
            self.stop_harvest_seeds_event.set()
            if self.restart_stream_timer:
                self.restart_stream_timer.cancel()
            if self.queue_warc_files_timer:
                self.queue_warc_files_timer.cancel()

        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)

        def pause(signal_number, stack_frame):
            self.is_pause = True

        signal.signal(signal.SIGUSR1, pause)

        log.debug("Message is %s" % json.dumps(self.message, indent=4))

        # Setup the restart timer for streams
        # The restart timer stops and restarts the stream periodically.
        # This makes makes sure that each HTTP response is limited in size.
        if self.is_streaming:
            self.restart_stream_timer = threading.Timer(self.stream_restart_interval_secs, self._restart_stream)
            self.restart_stream_timer.start()

        # Start a queue warc files timer
        self.queue_warc_files_timer = threading.Timer(self.queue_warc_files_interval_secs, self._queue_warc_files)
        self.queue_warc_files_timer.start()

        while not self.stop_harvest_loop_event.is_set():
            # Reset the stop_harvest_seeds_event
            self.stop_harvest_seeds_event = threading.Event()

            # If this isn't streaming then set stop_harvest_seeds_event so that looping doesn't occur.
            if not self.is_streaming:
                self.stop_harvest_loop_event.set()

            # Here is where the harvesting happens.
            try_count = 0
            done = False
            while not done:
                try_count += 1
                log.debug("Try {} of {}".format(try_count, self.tries))
                try:
                    if self.use_warcprox:
                        with warced(safe_string(self.message["id"]), self.warc_temp_dir, debug=self.debug_warcprox,
                                    interrupt=self.is_streaming,
                                    rollover_time=self.warc_rollover_secs if not self.is_streaming else None):
                            self.harvest_seeds()
                    else:
                        self.harvest_seeds()
                    done = True
                    log.debug("Done harvesting seeds.")
                except Exception as e:
                    log.exception("Unknown error raised during harvest: %s", e)
                    if try_count == self.tries:
                        # Give up trying
                        log.debug("Too many retries, so giving up on harvesting seeds.")
                        done = True
                        self.result.success = False
                        self.result.errors.append(Msg(CODE_UNKNOWN_ERROR, str(e)))
                        self.stop_harvest_loop_event.set()
                    else:
                        # Retry
                        # Queue any WARC files
                        self._queue_warc_files()
                        # Wait for any WARC files to be processed
                        log.debug("Waiting for processing to complete.")
                        self.warc_processing_queue.join()
                        log.debug("Processing complete.")

            # Queue any WARC files
            self._queue_warc_files()

        # Turn off the restart_stream_timer.
        if self.restart_stream_timer:
            self.restart_stream_timer.cancel()

        # Turn off the queue WARC files timer
        if self.queue_warc_files_timer:
            self.queue_warc_files_timer.cancel()

        # Finish processing
        self._finish_processing()

        # Delete temp dir
        if os.path.exists(self.warc_temp_dir):
            shutil.rmtree(self.warc_temp_dir)

        log.info("Done harvesting by message with id %s", self.message["id"])

    def _finish_processing(self):
        # Otherwise, will not get the last WARC on a stop.
        # No time is OK on a container kill because will resume and process last file.
        # Queue any new files.
        # Wait for processing to complete.
        log.debug("Waiting for processing to complete.")
        self.warc_processing_queue.join()
        log.debug("Processing complete.")

        if not self.is_pause:
            self.result.ended = datetime_now()

            # Send final message
            self._send_status_message(STATUS_SUCCESS if self.result.success else STATUS_FAILURE)

            # Delete result file
            if os.path.exists(self.result_filepath):
                os.remove(self.result_filepath)
        else:
            log.info("Pausing this harvest.")

            # Send final message
            self._send_status_message(STATUS_PAUSED)

    def _queue_warc_files(self):
        log.debug("Queueing WARC files")
        # Stop the timer
        if self.queue_warc_files_timer:
            self.queue_warc_files_timer.cancel()

        # Queue warc files
        for warc_filename in self._list_warcs(self.warc_temp_dir):
            log.debug("Queueing %s", warc_filename)
            self.warc_processing_queue.put(warc_filename)

        # Restart the timer
        if self.queue_warc_files_timer:
            self.queue_warc_files_timer = threading.Timer(self.queue_warc_files_interval_secs, self._queue_warc_files)
            self.queue_warc_files_timer.start()

    def harvest_from_file(self, filepath, is_streaming=False, delete=False):
        """
        Performs a harvest based on the a harvest start message contained in the
        provided filepath.

        :param filepath: filepath of the harvest start message
        :param is_streaming: True to run in streaming mode
        :param delete: True to delete when completed
        """
        self.is_streaming = is_streaming
        return self.message_from_file(filepath, delete=delete)

    def harvest_seeds(self):
        """
        Performs a harvest based on the seeds contained in the message.

        When called, self.message, self.routing_key,
        and self.is_streaming will be populated.
        """
        pass

    def _create_state_store(self):
        """
        Creates a state store for the harvest.
        """
        # We'll be delaying writing to the state store until done processing the warc file.
        self.state_store = DelayedSetStateStoreAdapter(JsonHarvestStateStore(self.message["path"]))

    @staticmethod
    def _list_warcs(path):
        warcs = []
        if os.path.exists(path):
            warcs = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and
                     (f.endswith(".warc") or f.endswith(".warc.gz"))]
            log.debug("Found following WARCs in %s: %s", path, warcs)
        else:
            log.warning("Warc path %s does not exist. This may be OK.", path)
        return warcs

    @staticmethod
    def _path_for_warc(harvest_path, filename):
        m = re.search("-(\d{4})(\d{2})(\d{2})(\d{2})\d{7}-", filename)
        assert m
        return "/".join([harvest_path, m.group(1), m.group(2), m.group(3), m.group(4)])

    def _send_status_message(self, status):
        message = {
            "id": self.message["id"],
            "status": status,
            "infos": [msg.to_map() for msg in self.result.infos],
            "warnings": [msg.to_map() for msg in self.result.warnings],
            "errors": [msg.to_map() for msg in self.result.errors],
            "date_started": self.result.started.isoformat(),
            "stats": dict(),
            "token_updates": self.result.token_updates,
            "uids": self.result.uids,
            "warcs": {
                "count": len(self.result.warcs),
                "bytes": self.result.warc_bytes
            },
            # This will add spaces before caps
            "service": re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', self.__class__.__name__),
            "host": self.host,
            "instance": str(os.getpid())
        }

        for day, stats in self.result.stats().items():
            message["stats"][day.isoformat()] = dict(stats)

        if self.result.ended:
            message["date_ended"] = self.result.ended.isoformat()

        # Routing key may be none
        log.info("Sending status message for harvest %s: %s", self.message["id"], status)
        status_routing_key = self.routing_key.replace("start", "status")
        self._publish_message(status_routing_key, message)

    @staticmethod
    def _clean_name(name):
        re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', name)

    def _send_warc_created_message(self, warc_path):
        message = {
            "harvest": {
                "id": self.message["id"],
                "type": self.message["type"]
            },
            "collection_set": {
                "id": self.message["collection_set"]["id"]
            },
            "collection": {
                "id": self.message["collection"]["id"]
            },
            "warc": {
                "id": uuid.uuid4().hex,
                "path": warc_path,
                "date_created": datetime_from_stamp(os.path.getctime(warc_path)).isoformat(),
                "bytes": os.path.getsize(warc_path),
                "sha1": hashlib.sha1(open(warc_path, 'rb').read()).hexdigest()
            }
        }
        self._publish_message("warc_created", message)

    def _create_warc_temp_dir(self):
        """
        Create temporary directory for WARC files.

        :return: the directory path
        """
        path = os.path.join(self.working_path, "tmp", safe_string(self.message["id"]))
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    def _restart_stream(self):
        log.debug("Restarting stream.")
        self.stop_harvest_seeds_event.set()
        self.restart_stream_timer = threading.Timer(self.stream_restart_interval_secs, self._restart_stream)
        self.restart_stream_timer.start()

    def _save_result(self):
        result_message = {
            "warcs": self.result.warcs,
            "warc_bytes": self.result.warc_bytes,
            "stats": [],
            "started": self.result.started.isoformat(),
            "infos": [msg.to_map() for msg in self.result.infos],
            "warnings": [msg.to_map() for msg in self.result.warnings],
            "errors": [msg.to_map() for msg in self.result.errors]

        }

        for day, stats in self.result.stats().items():
            result_message["stats"].append((day.isoformat(), dict(stats)))

        with codecs.open(self.result_filepath, 'w') as f:
            json.dump(result_message, f)

        log.debug("Persisted result to %s", self.result_filepath)

    def _load_result(self):
        if os.path.exists(self.result_filepath):
            log.info("Resuming from previous results")
            with codecs.open(self.result_filepath, 'r') as f:
                result_message = json.load(f)
            log.debug("Previous results: {}".format(json.dumps(result_message, indent=4)))
            self.result.warcs = result_message["warcs"]
            self.result.warc_bytes = result_message["warc_bytes"]
            self.result.started = iso8601.parse_date(result_message["started"])
            self.result.infos = list([Msg(msg["code"], msg["message"]) for msg in result_message["infos"]])
            self.result.warnings = list([Msg(msg["code"], msg["message"]) for msg in result_message["warnings"]])
            self.result.errors = list([Msg(msg["code"], msg["message"]) for msg in result_message["errors"]])

            for day, stats in result_message["stats"]:
                for item, count in stats.items():
                    self.result.increment_stats(item, count=count, day=iso8601.parse_date(day).date())

    def _process_warc_thread(self):
        log.info("Starting WARC processing thread")
        # This will continue until harvester is killed.
        while True:
            # This will block
            try:
                warc_filename = self.warc_processing_queue.get(timeout=1)
            except Empty:
                continue
            # Make sure file exists. Possible that same file will be put in queue multiple times.
            warc_filepath = os.path.join(self.warc_temp_dir, warc_filename)
            if os.path.exists(warc_filepath):
                # Process the warc
                self.process_warc(warc_filepath)

                # Move the warc
                dest_path = self._path_for_warc(self.message["path"], warc_filename)
                dest_warc_filepath = os.path.join(dest_path, warc_filename)
                log.debug("Moving %s to %s", warc_filepath, dest_warc_filepath)
                if not os.path.exists(dest_path):
                    os.makedirs(dest_path)
                shutil.move(warc_filepath, dest_warc_filepath)

                # Persist the state
                self.state_store.pass_state()

                # Add it to result
                self.result.add_warc(dest_warc_filepath)

                # Send warc created message
                self._send_warc_created_message(dest_warc_filepath)

                # Send status message
                self._send_status_message(STATUS_STOPPING if self.stop_harvest_seeds_event.is_set() else STATUS_RUNNING)

                # Since these were sent, clear them.
                self.result.token_updates = {}
                self.result.uids = {}

                # Persist the result for resuming
                self._save_result()

            else:
                log.debug("Skipping processing %s", warc_filename)
            # Mark this as done.
            self.warc_processing_queue.task_done()

    def on_persist_exception(self, exception):
        log.error("Handling on persist exception for %s", self.message["id"])
        message = {
            "id": self.message["id"],
            "status": STATUS_FAILURE,
            "errors": [Msg(CODE_MSG_PERSIST_ERROR, str(exception)).to_map()],
            "date_started": datetime_now().isoformat(),
            "date_ended": datetime_now().isoformat(),
            # This will add spaces before caps
            "service": re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', self.__class__.__name__),
            "host": self.host,
            "instance": str(os.getpid())
        }

        # Routing key may be none
        status_routing_key = self.routing_key.replace("start", "status")
        self._publish_message(status_routing_key, message)

    def process_warc(self, warc_filepath):
        """
        Processes the provided WARC file.

        Processing involves:
        * Save state to self.state_store.
        * Increment counts in self.result.
        """
        pass

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
        # Logging
        logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

        # Arguments
        parser = argparse.ArgumentParser()
        parser.add_argument("--debug", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                            default="False", const="True")
        parser.add_argument("--debug-http", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                            default="False", const="True")
        parser.add_argument("--debug-warcprox", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                            default="False", const="True")

        subparsers = parser.add_subparsers(dest="command")

        service_parser = subparsers.add_parser("service", help="Run harvesting service that consumes messages from "
                                                               "messaging queue.")
        service_parser.add_argument("host")
        service_parser.add_argument("username")
        service_parser.add_argument("password")
        service_parser.add_argument("working_path")
        service_parser.add_argument("--skip-resume", action="store_true")
        service_parser.add_argument("--tries", type=int, default="3", help="Number of times to try harvests if errors.")
        service_parser.add_argument("--priority-queues", type=lambda v: v.lower() in ("yes", "true", "t", "1"),
                                    nargs="?", default="False", const="True")

        seed_parser = subparsers.add_parser("seed", help="Harvest based on a seed file.")
        seed_parser.add_argument("filepath", help="Filepath of the seed file.")
        seed_parser.add_argument("working_path")
        seed_parser.add_argument("--streaming", action="store_true", help="Run in streaming mode.")
        seed_parser.add_argument("--host")
        seed_parser.add_argument("--username")
        seed_parser.add_argument("--password")
        seed_parser.add_argument("--tries", type=int, default="3", help="Number of times to try harvests if errors.")

        args = parser.parse_args()

        # Logging
        logging.getLogger("requests").setLevel(logging.DEBUG if args.debug_http else logging.INFO)
        logging.getLogger("requests_oauthlib").setLevel(logging.DEBUG if args.debug_http else logging.INFO)
        logging.getLogger("oauthlib").setLevel(logging.DEBUG if args.debug_http else logging.INFO)
        logging.getLogger("urllib3").setLevel(logging.DEBUG if args.debug_http else logging.INFO)

        if args.command == "service":
            # Optionally add priority to queues
            if args.priority_queues:
                for i, key in enumerate(routing_keys):
                    routing_keys[i] = key + ".priority"
                queue += "_priority"

            harvester = cls(args.working_path, mq_config=MqConfig(args.host, args.username, args.password, EXCHANGE,
                                                                  {queue: routing_keys}),
                            debug=args.debug, debug_warcprox=args.debug_warcprox, tries=args.tries)
            if not args.skip_resume:
                harvester.resume_from_file()
            harvester.run()
        elif args.command == "seed":
            mq_config = MqConfig(args.host, args.username, args.password, EXCHANGE, None) \
                if args.host and args.username and args.password else None
            harvester = cls(args.working_path, mq_config=mq_config, debug=args.debug,
                            debug_warcprox=args.debug_warcprox, tries=args.tries)
            harvester.harvest_from_file(args.filepath, is_streaming=args.streaming)
            if __name__ == '__main__':
                if harvester.result:
                    log.info("Result is: %s", harvester.result)
                    sys.exit(0)
                else:
                    log.warning("Result is: %s", harvester.result)
                    sys.exit(1)
