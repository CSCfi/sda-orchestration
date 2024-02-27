"""Message Broker inbox step consumer."""

import json
from typing import Dict
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
from os import environ
from pathlib import Path
from jsonschema.exceptions import ValidationError
from .schemas.validate import ValidateJSON, load_schema


class InboxConsumer(Consumer):
    """Inbox Consumer class."""

    def handle_message(self, message: Message) -> None:
        """Handle message."""
        try:
            inbox_msg = json.loads(message.body)

            LOG.debug(f"MQ Message body: {message.body} .")
            LOG.debug(f"Inbox Consumer message received: {inbox_msg} .")
            LOG.info(
                f"Received work (corr-id: {message.correlation_id} filepath: {inbox_msg['filepath']},"
                f"user: {inbox_msg['user']} with operation: {inbox_msg['operation']})",
            )

            if inbox_msg["operation"] == "upload":
                ValidateJSON(load_schema("inbox-upload")).validate(inbox_msg)
                # we check if this is a path with a suffix or a name
                test_path = Path(inbox_msg["filepath"])
                if test_path.name in ["", ".", ".."]:
                    LOG.error(f"file: {test_path} does not appear to be a correct path.")
                    raise FileNotFoundError

                # Create the files message.
                # we keep the encrypted_checksum but it can also be missing
                self._publish_ingest(message, inbox_msg)
            elif inbox_msg["operation"] == "rename":
                ValidateJSON(load_schema("inbox-rename")).validate(inbox_msg)
                pass
            elif inbox_msg["operation"] == "remove":
                ValidateJSON(load_schema("inbox-remove")).validate(inbox_msg)
                pass
            else:
                LOG.error("Un-identified inbox operation.")
                pass

        except ValidationError:
            LOG.error("Could not validate the inbox message. Not properly formatted.")
            raise

        except Exception as error:
            LOG.error(f"Error occurred in inbox consumer: {error}.")
            raise

    def _publish_ingest(self, message: Message, inbox_msg: Dict) -> None:
        """Publish message with dataset to accession ID mapping."""
        properties = {
            "content_type": "application/json",
            "headers": {},
            "correlation_id": message.correlation_id,
            "delivery_mode": 2,
        }
        try:
            channel = self.connection.channel()  # type: ignore

            ingest_trigger = {"type": "ingest", "user": inbox_msg["user"], "filepath": inbox_msg["filepath"]}
            if "encrypted_checksums" in inbox_msg:
                ingest_trigger["encrypted_checksums"] = inbox_msg["encrypted_checksums"]

            ingest_msg = json.dumps(ingest_trigger)
            ValidateJSON(load_schema("ingestion-trigger")).validate(json.loads(ingest_msg))

            ingest = Message.create(channel, ingest_msg, properties)

            ingest.publish(environ.get("INGEST_QUEUE", "ingest"), exchange=environ.get("BROKER_EXCHANGE", "sda"))
            channel.close()

            LOG.info(f'Sent the message to ingest queue to trigger ingestion for filepath: {inbox_msg["filepath"]}.')

        except ValidationError:
            LOG.error("Could not validate the ingest trigger message. Not properly formatted.")
            raise Exception("Could not validate the ingest trigger message. Not properly formatted.")


def main() -> None:
    """Run the Inbox consumer."""
    CONSUMER = InboxConsumer(
        hostname=str(environ.get("BROKER_HOST")),
        port=int(environ.get("BROKER_PORT", 5670)),
        username=environ.get("BROKER_USER", "sda"),
        password=environ.get("BROKER_PASSWORD", ""),
        queue=environ.get("INBOX_QUEUE", "inbox"),
        vhost=environ.get("BROKER_VHOST", "sda"),
    )
    CONSUMER.start()


if __name__ == "__main__":
    main()
