#!/usr/bin/env python3

from __future__ import absolute_import
import socket
import logging
import json
import argparse
import signal
from sfmutils.consumer import BaseConsumer, MqConfig, EXCHANGE
from sfmutils.supervisor import HarvestSupervisor

log = logging.getLogger(__name__)


class StreamConsumer(BaseConsumer):
    """
    A consumer intended to control stream harvests using Supervisor.

    When it receives a harvest start message, it starts a supervisor
    process for harvesting the message.

    When it receives a harvest stop message, it removes the supervisor process
    for the harvest.

    Logs for the supervisor processes are in /var/log/sfm.
    """

    def __init__(self, script, working_path, debug=False, mq_config=None, debug_warcprox=False, tries=3):
        BaseConsumer.__init__(self, working_path=working_path, mq_config=mq_config)
        # Add routing keys for harvest stop messages
        # The queue will be unique to this instance of StreamServer so that it
        # will receive all stop requests
        if mq_config:
            for queue, routing_keys in list(mq_config.queues.items()):
                mq_config.queues["_".join([queue, socket.gethostname()])] = [routing_key.replace("start", "stop")
                                                                             for routing_key in routing_keys]
            log.debug("Queues are now %s", mq_config.queues)

        self.message = None
        self.debug = debug
        self.debug_warcprox = debug_warcprox
        self.tries = tries
        self._supervisor = HarvestSupervisor(script, mq_config.host, mq_config.username, mq_config.password,
                                             working_path, debug=debug, process_owner="sfm")

        # Shutdown Supervisor.
        def shutdown(signal_number, stack_frame):
            log.debug("Shutdown triggered")
            self._supervisor.pause_all()
            self.should_stop = True
        log.debug("Registering shutdown signal")

        signal.signal(signal.SIGTERM, shutdown)
        signal.signal(signal.SIGINT, shutdown)

    def on_message(self):
        harvest_id = self.message["id"]
        if self.routing_key.startswith("harvest.start."):
            # Start
            log.info("Starting %s", harvest_id)
            log.debug("Message for %s is %s", harvest_id, json.dumps(self.message, indent=4))
            self._supervisor.start(self.message, self.routing_key, debug=self.debug, debug_warcprox=self.debug_warcprox,
                                   tries=self.tries)
        else:
            # Stop
            log.info("Stopping %s", harvest_id)
            self._supervisor.remove(harvest_id)


if __name__ == "__main__":
    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("username")
    parser.add_argument("password")
    parser.add_argument("queue")
    parser.add_argument("routing_keys", help="Comma separated list of routing keys")
    parser.add_argument("script")
    parser.add_argument("working_path")
    parser.add_argument("--debug", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                        default="False", const="True")
    parser.add_argument("--debug-warcprox", type=lambda v: v.lower() in ("yes", "true", "t", "1"), nargs="?",
                        default="False", const="True")
    parser.add_argument("--tries", type=int, default="3", help="Number of times to try harvests if errors.")

    args = parser.parse_args()

    # Logging
    logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s',
                        level=logging.DEBUG if args.debug else logging.INFO)

    consumer = StreamConsumer(args.script, args.working_path,
                              mq_config=MqConfig(args.host,
                                                 args.username,
                                                 args.password,
                                                 EXCHANGE,
                                                 {args.queue: args.routing_keys.split(",")}),
                              debug=args.debug, debug_warcprox=args.debug_warcprox, tries=args.tries)
    consumer.run()
