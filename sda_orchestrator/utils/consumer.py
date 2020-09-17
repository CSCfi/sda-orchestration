"""Message Broker Consumer class."""

import time
from amqpstorm import Connection, AMQPError
from .logger import LOG
import ssl
from pathlib import Path


class Consumer:
    """CEGA message consumer."""

    def __init__(
        self,
        hostname="localhost",
        username="guest",
        password=None,
        port=5671,
        queue="base.queue",
        max_retries=None,
        vhost="/",
    ):
        """Consumer init function."""
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.queue = queue
        self.vhost = vhost
        self.max_retries = max_retries
        self.connection = None
        context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
        context.check_hostname = False
        cacertfile = Path("/tls/certs/root.ca.crt")
        certfile = Path("/tls/certs/cert.ca.crt")
        keyfile = Path("/tls/certs/cert.ca.key")
        context.verify_mode = ssl.CERT_NONE
        # Require server verification
        if cacertfile.exists():
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cafile=str(cacertfile))
        # If client verification is required
        if certfile.exists():
            context.load_cert_chain(str(certfile), keyfile=str(keyfile))
        self.ssl_context = {"context": context, "server_hostname": None, "check_hostname": False}

    def create_connection(self):
        """Create a connection.

        :return:
        """
        attempts = 0
        while True:
            attempts += 1
            try:
                self.connection = Connection(
                    self.hostname,
                    self.username,
                    self.password,
                    port=self.port,
                    ssl=True,
                    ssl_options=self.ssl_context,
                    virtual_host=self.vhost,
                )
                LOG.info("Established connection with AMQP server {0}".format(self.connection))
                break
            except AMQPError as error:
                LOG.error("Something went wrong: {0}".format(error))
                if self.max_retries and attempts > self.max_retries:
                    break
                time.sleep(min(attempts * 2, 30))
            except KeyboardInterrupt:
                break

    def start(self):
        """Start the Consumer.

        :return:
        """
        if not self.connection:
            self.create_connection()
        while True:
            try:
                channel = self.connection.channel()
                # channel.queue.declare(self.queue)
                channel.basic.consume(self, self.queue, no_ack=False)
                LOG.info("Connected to queue {0}".format(self.queue))
                channel.start_consuming(to_tuple=False)
                if not channel.consumer_tags:
                    channel.close()
            except AMQPError as error:
                LOG.error("Something went wrong: {0}".format(error))
                self.create_connection()
            except KeyboardInterrupt:
                self.connection.close()
                break

    def handle_message(self, message):
        """Handle message."""
        pass

    def __call__(self, message):
        """Process the message body."""
        try:
            self.handle_message(message)
        except Exception as error:
            LOG.error("Something went wrong: {0}".format(error))
            message.reject(requeue=False)
        else:
            message.ack()
