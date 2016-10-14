import logging
from kombu import Connection, Queue, Exchange, Producer
from kombu.mixins import ConsumerMixin
import json
import os
import codecs

log = logging.getLogger(__name__)

EXCHANGE = "sfm_exchange"


# Copied from https://github.com/celery/kombu/blob/master/kombu/mixins.py until in a kombu release.
class ConsumerProducerMixin(ConsumerMixin):
    """Version of ConsumerMixin having separate connection for also
    publishing messages.
    Example:
    .. code-block:: python
        class Worker(ConsumerProducerMixin):
            def __init__(self, connection):
                self.connection = connection
            def get_consumers(self, Consumer, channel):
                return [Consumer(queues=Queue('foo'),
                                 on_message=self.handle_message,
                                 accept='application/json',
                                 prefetch_count=10)]
            def handle_message(self, message):
                self.producer.publish(
                    {'message': 'hello to you'},
                    exchange='',
                    routing_key=message.properties['reply_to'],
                    correlation_id=message.properties['correlation_id'],
                    retry=True,
                )
    """
    _producer_connection = None

    def on_consume_end(self, connection, channel):
        if self._producer_connection is not None:
            self._producer_connection.close()
            self._producer_connection = None

    @property
    def producer(self):
        return Producer(self.producer_connection)

    @property
    def producer_connection(self):
        if self._producer_connection is None:
            conn = self.connection.clone()
            conn.ensure_connection(self.on_connection_error,
                                   self.connect_max_retries)
            self._producer_connection = conn
        return self._producer_connection


class BaseConsumer(ConsumerProducerMixin):
    """
    Base class for consuming messages from Rabbit.

    A BaseConsumer can be configured with an exchange and a mapping of
    queues to routing keys. Exchanges, queues, and bindings will
    be automatically created.

    Subclasses should override on_message().

    To send a message, use self.producer.publish().
    """
    def __init__(self, mq_config=None, persist_messages=False, working_path=None):
        self.mq_config = mq_config
        if self.mq_config and self.mq_config.host and self.mq_config.username and self.mq_config.password:
            self.connection = Connection(transport="librabbitmq",
                                         hostname=mq_config.host,
                                         userid=mq_config.username,
                                         password=mq_config.password)
            self.exchange = Exchange(name=self.mq_config.exchange,
                                     type="topic",
                                     durable=True)
        else:
            self.connection = None
            self.exchange = None

        self.message_filepath = None
        self.persist_messages = persist_messages
        assert persist_messages == False or working_path
        self.working_path = working_path
        if self.working_path:
            if not os.path.exists(self.working_path):
                os.makedirs(self.working_path)
            log.debug("Temporary path is %s", self.working_path)
            self.message_filepath = os.path.join(self.working_path, "last_message.json")
        self.message = None
        self.routing_key = None

        self.result = None

    def get_consumers(self, Consumer, channel):
        assert self.mq_config

        # Declaring ourselves rather than use auto-declare.
        log.debug("Declaring %s exchange", self.mq_config.exchange)
        self.exchange(channel).declare()

        queues = []
        for queue_name, routing_keys in self.mq_config.queues.items():
            queue = Queue(name=queue_name,
                          exchange=self.exchange,
                          channel=channel,
                          durable=True)
            log.debug("Declaring queue %s", queue_name)
            queue.declare()
            for routing_key in routing_keys:
                log.debug("Binding queue %s to %s", queue_name, routing_key)
                queue.bind_to(exchange=self.exchange,
                              routing_key=routing_key)
            queues.append(queue)

        consumer = Consumer(queues=queues,
                            callbacks=[self._callback],
                            auto_declare=False)
        consumer.qos(prefetch_count=1, apply_global=True)
        return [consumer]

    def _callback(self, message, message_obj):
        """
        Callback for receiving harvest message.
        """
        self.routing_key = message_obj.delivery_info["routing_key"]
        self.message = message

        # Persist the message
        if self.persist_messages:
            with codecs.open(self.message_filepath, 'w') as f:
                json.dump({
                    "routing_key": self.routing_key,
                    "message": self.message
                }, f)
            log.debug("Persisted message to %s", self.message_filepath)

        # Acknowledge the message
        message_obj.ack()

        # Don't want to get in a loop, so when an exception occurs, delete the message.
        try:
            self.on_message()
        finally:
            # Delete the message
            if self.persist_messages and os.path.exists(self.message_filepath):
                os.remove(self.message_filepath)
                log.debug("Deleted %s", self.message_filepath)

    def on_message(self):
        """
        Override this class to consume message.

        When called, self.routing_key and self.message
        will be populated based on the new message.
        """
        pass

    def message_from_file(self, filepath, delete=False):
        """
        Loads message from file and invokes on_message().

        :param filepath: filepath of the message
        :param delete: If True, deletes after on_message() is completed
        """
        log.info("Loading from file %s", filepath)

        with codecs.open(filepath, "r") as f:
            msg_container = json.load(f)

        self.routing_key = msg_container['routing_key']
        self.message = msg_container['message']
        self.message_filepath = filepath

        self.on_message()

        # Delete the message
        if delete:
            os.remove(self.message_filepath)
            log.debug("Deleted %s", self.message_filepath)
        if self.result is not None:
            return self.result

    def resume_from_file(self):
        """
        If a persisted message exists, invoke that message.
        """
        if os.path.exists(self.message_filepath):
            log.info("%s exists, so resuming", self.message_filepath)
            self.message_from_file(self.message_filepath, delete=True)
        else:
            log.info("%s does not exist, so not resuming", self.message_filepath)

    def _publish_message(self, routing_key, message, trunate_debug_length=None):
        message_body = json.dumps(message, indent=4)
        if self.mq_config:
            if trunate_debug_length:
                log.debug("Sending message to %s with routing_key %s. The first %s characters of the body is: %s",
                          self.exchange.name, routing_key,
                          trunate_debug_length, message_body[:trunate_debug_length])
            else:
                log.debug("Sending message to %s with routing_key %s. The body is: %s", self.exchange.name, routing_key,
                          message_body)
            self.producer.publish(body=message,
                                  routing_key=routing_key,
                                  retry=True,
                                  exchange=self.exchange)
        else:
            if trunate_debug_length:
                log.debug(
                    "Skipping sending message to sfm_exchange with routing_key %s. The first %s characters of the body "
                    "is: %s",
                    routing_key, trunate_debug_length, message_body[:trunate_debug_length])
            else:
                log.debug("Skipping sending message to sfm_exchange with routing_key %s. The body is: %s",
                          routing_key, message_body)


class MqConfig:
    """
    Configuration for connecting to RabbitMQ.
    """
    def __init__(self, host, username, password, exchange, queues):
        """
        :param host: the host
        :param username: the username
        :param password: the password
        :param exchange: the exchange
        :param queues: map of queue names to lists of routing keys
        """
        self.host = host
        self.username = username
        self.password = password
        self.exchange = exchange
        self.queues = queues
