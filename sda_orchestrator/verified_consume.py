"""Message Broker verify step consumer."""
import json
from typing import Dict
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
from os import environ
from .utils.id_ops import generate_accession_id
from jsonschema.exceptions import ValidationError
from .schemas.validate import ValidateJSON, load_schema


class VerifyConsumer(Consumer):
    """Verify Consumer class."""

    def handle_message(self, message: Message) -> None:
        """Handle message."""
        try:
            verify_msg = json.loads(message.body)

            LOG.debug(f"MQ Message body: {message.body} .")
            LOG.debug(f"Verify Consumer message received: {verify_msg} .")
            LOG.info(
                f"Received work (corr-id: {message.correlation_id} filepath: {verify_msg['filepath']}, \
                user: {verify_msg['user']}, \
                decryptedChecksums: {verify_msg['decrypted_checksums']})",
            )

            ValidateJSON(load_schema("ingestion-accession-request")).validate(verify_msg)

            accessionID = generate_accession_id()
            self._publish_accessionID(message, accessionID, verify_msg)

        except ValidationError:
            LOG.error("Could not validate the verify message. Not properly formatted.")
            raise
        except Exception as error:
            LOG.error(f"Error occurred in verify consumer: {error}.")
            raise

    def _publish_accessionID(self, message: Message, accessionID: str, verify_msg: Dict) -> None:
        """Publish message with dataset to accession ID mapping."""
        properties = {
            "content_type": "application/json",
            "headers": {},
            "correlation_id": message.correlation_id,
            "delivery_mode": 2,
        }
        try:
            # Create the message.
            channel = self.connection.channel()  # type: ignore

            accession_trigger = {
                "type": "accession",
                "user": verify_msg["user"],
                "filepath": verify_msg["filepath"],
                "decrypted_checksums": verify_msg["decrypted_checksums"],
                "accession_id": accessionID,
            }

            accession_msg = json.dumps(accession_trigger)
            ValidateJSON(load_schema("ingestion-accession")).validate(json.loads(accession_msg))

            accession = Message.create(channel, accession_msg, properties)
            checksum_data = list(filter(lambda x: x["type"] == "sha256", verify_msg["decrypted_checksums"]))
            decrypted_checksum = checksum_data[0]["value"]
            accession.publish(
                environ.get("ACCESSIONIDS_QUEUE", "accessionIDs"), exchange=environ.get("BROKER_EXCHANGE", "sda")
            )

            channel.close()
            LOG.info(
                f'Sent the message to accessionIDs queue to set accession ID for file {verify_msg["filepath"]} \
                     with checksum {decrypted_checksum}.'
            )

        except ValidationError:
            LOG.error("Could not validate the ingestion accession message. Not properly formatted.")
            raise Exception("Could not validate the ingestion accession message. Not properly formatted.")


def main() -> None:
    """Run the Verify consumer."""
    CONSUMER = VerifyConsumer(
        hostname=str(environ.get("BROKER_HOST")),
        port=int(environ.get("BROKER_PORT", 5670)),
        username=environ.get("BROKER_USER", "sda"),
        password=environ.get("BROKER_PASSWORD", ""),
        queue=environ.get("VERIFIED_QUEUE", "verified"),
        vhost=environ.get("BROKER_VHOST", "sda"),
    )
    CONSUMER.start()


if __name__ == "__main__":
    main()
