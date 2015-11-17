from __future__ import absolute_import
import socket
import logging
import json
import argparse
from sfmutils.harvester import BaseConsumer, MqConfig, EXCHANGE
from sfmutils.supervisor import HarvestSupervisor

log = logging.getLogger(__name__)


class StreamConsumer(BaseConsumer):
    def __init__(self, mq_config, script):
        BaseConsumer.__init__(self, mq_config)
        # Add routing keys for harvest stop messages
        # The queue will be unique to this instance of StreamServer so that it
        # will receive all stop requests
        if mq_config:
            for queue, routing_keys in mq_config.queues.items():
                mq_config.queues["_".join([queue, socket.gethostname()])] = [routing_key.replace("start", "stop")
                                                                             for routing_key in routing_keys]
            log.debug("Queues are now %s", mq_config.queues)

        self.message = None
        self._supervisor = HarvestSupervisor(script, mq_config.host, mq_config.username, mq_config.password)

    def harvest(self):
        self.message = json.loads(self.message_body)
        harvest_id = self.message["id"]
        if self.routing_key.startswith("harvest.start."):
            #Start
            log.info("Starting %s", harvest_id)
            log.debug("Message for %s is %s", harvest_id, self.message_body)
            self._supervisor.start(self.message, self.routing_key)
        else:
            #Stop
            log.info("Stopping %s", harvest_id)
            self._supervisor.stop(harvest_id)

if __name__ == "__main__":
    #Logging
    logging.basicConfig(format='%(asctime)s: %(name)s --> %(message)s', level=logging.DEBUG)

    #Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("host")
    parser.add_argument("username")
    parser.add_argument("password")
    parser.add_argument("queue")
    parser.add_argument("routing_keys", help="Comma separated list of routing keys")
    parser.add_argument("script")

    args = parser.parse_args()

    consumer = StreamConsumer(MqConfig(args.host, args.username, args.password, EXCHANGE,
                                           {args.queue: args.routing_keys.split(",")}), args.script)
    consumer.consume()
