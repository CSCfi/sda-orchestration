"""Message Broker Consumer class."""

import time
from os import environ
import json
import ssl
from pathlib import Path
from typing import Union
from distutils.util import strtobool

from amqpstorm import Connection, AMQPError, Message

from .logger import LOG
from jsonschema.exceptions import ValidationError
from ..schemas.validate import ValidateJSON, load_schema


class Consumer:
    """CEGA message consumer."""

    def __init__(
        self,
        hostname: str = "localhost",
        username: str = "guest",
        password: Union[None, str] = None,
        port: int = 5671,
        queue: str = "base.queue",
        max_retries: Union[None, int] = None,
        vhost: str = "/",
    ) -> None:
        """Consumer init function."""
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.queue = queue
        self.vhost = vhost
        self.max_retries = max_retries
        self.connection = None
        self.ssl = bool(strtobool(environ.get("BROKER_SSL", "True")))
        context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2)
        context.check_hostname = False
        cacertfile = Path(environ.get("SSL_CACERT", "/tls/certs/ca.crt"))
        certfile = Path(environ.get("SSL_CLIENTCERT", "/tls/certs/orch.crt"))
        keyfile = Path(environ.get("SSL_CLIENTKEY", "/tls/certs/orch.key"))
        context.verify_mode = ssl.CERT_NONE
        # Require server verification
        if cacertfile.exists():
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cafile=str(cacertfile))
        # If client verification is required
        if certfile.exists():
            context.load_cert_chain(str(certfile), keyfile=str(keyfile))
        self.ssl_context = {"context": context, "server_hostname": None, "check_hostname": False}

    def create_connection(self) -> None:
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
                    ssl=self.ssl,
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

    def start(self) -> None:
        """Start the Consumer.

        :return:
        """
        if not self.connection:
            self.create_connection()
        while True:
            try:
                channel = self.connection.channel()  # type: ignore
                channel.basic.consume(self, self.queue, no_ack=False)
                LOG.info("Connected to queue {0}".format(self.queue))
                channel.start_consuming(to_tuple=False)
                if not channel.consumer_tags:
                    channel.close()
            except AMQPError as error:
                LOG.error("Something went wrong: {0}".format(error))
                self.create_connection()
            except KeyboardInterrupt:
                self.connection.close()  # type: ignore
                break

    def handle_message(self, message: Message) -> None:
        """Handle message."""
        pass

    def _error_message(self, message: Message, reason: str) -> None:
        """Send formated error message to error queue."""
        channel = self.connection.channel()  # type: ignore
        properties = {
            "content_type": "application/json",
            "headers": {},
            "correlation_id": message.correlation_id,
            "delivery_mode": 2,
        }
        original_message = json.loads(message.body)

        error_trigger = {"user": original_message["user"], "filepath": original_message["filepath"], "reason": reason}

        if "encrypted_checksums" in original_message:
            error_trigger["encrypted_checksums"] = original_message["encrypted_checksums"]

        if "decrypted_checksums" in original_message:
            error_trigger["decrypted_checksums"] = original_message["decrypted_checksums"]

        error_msg = json.dumps(error_trigger)
        LOG.debug(f"Error Message: {error_msg}")
        ValidateJSON(load_schema("ingestion-user-error")).validate(json.loads(error_msg))

        error = Message.create(channel, error_msg, properties)
        error.publish(environ.get("ERROR_QUEUE", "error"), exchange=environ.get("BROKER_EXCHANGE", "sda"))

        channel.close()

        LOG.info(
            f"Published error message (corr-id: {message.correlation_id} filepath: {original_message['filepath']}, ",
            f"user: {original_message['user']}, with reason: {reason})",
        )

    def __call__(self, message: Message) -> None:
        """Process the message body."""
        try:
            self.handle_message(message)
        except (ValidationError, Exception) as error:
            try:
                self._error_message(message, f"Exception occurred: {error}")
            except ValidationError:
                LOG.error("Could not validate the error message. Not properly formatted.")
            except Exception as error:
                LOG.error(f"Exception occurred: {error}")
            finally:
                message.reject(requeue=False)
        else:
            message.ack()
