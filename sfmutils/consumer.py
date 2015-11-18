import pika
import logging

log = logging.getLogger(__name__)

EXCHANGE = "sfm_exchange"


class BaseConsumer():
    def __init__(self, mq_config):
        self.mq_config = mq_config
        self._connection = None

        #Creating a connection can be skipped for testing purposes
        if not mq_config.skip_connection:
            credentials = pika.PlainCredentials(mq_config.username, mq_config.password)
            parameters = pika.ConnectionParameters(host=mq_config.host, credentials=credentials)
            self._connection = pika.BlockingConnection(parameters)
            channel = self._connection.channel()
            #Declare sfm_exchange
            channel.exchange_declare(exchange=mq_config.exchange,
                                     type="topic", durable=True)
            channel.close()

    def consume(self):
        assert self._connection

        channel = self._connection.channel()

        for queue, routing_keys in self.mq_config.queues.items():
            #Declare harvester queue
            channel.queue_declare(queue=queue,
                                  durable=True)
            #Bind
            for routing_key in routing_keys:
                channel.queue_bind(exchange=self.mq_config.exchange,
                                   queue=queue, routing_key=routing_key)

        channel.basic_qos(prefetch_count=1)
        channel.basic_qos(prefetch_count=1, all_channels=True)
        for queue in self.mq_config.queues.keys():
            log.info("Waiting for messages from %s", queue)
            channel.basic_consume(self._callback, queue=queue)

        channel.start_consuming()

    def _callback(self, channel, method, _, body):
        """
        Callback for receiving harvest message.

        Note that channel and method can be None to allow invoking from a
        non-mq environment.
        """
        self.channel = channel
        self.routing_key = method.routing_key
        self.message_body = body

        #Acknowledge the message
        if channel:
            log.debug("Acking message")
            channel.basic_ack(delivery_tag=method.delivery_tag)

        self.on_message()

    def on_message(self):
        pass


class MqConfig():
    """
    Configuration for connecting to RabbitMQ.
    """
    def __init__(self, host, username, password, exchange, queues, skip_connection=False):
        """
        :param host: the host
        :param username: the username
        :param password: the password
        :param exchange: the exchange
        :param queues: map of queue names to lists of routing keys
        :param skip_connection: if True, skip creating connection (for testing)
        """
        self.host = host
        self.username = username
        self.password = password
        self.exchange = exchange
        self.queues = queues
        self.skip_connection = skip_connection