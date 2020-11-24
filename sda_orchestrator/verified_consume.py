"""Message Broker verify step consumer."""
import json
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
import os
from .utils.id_ops import generate_accession_id


class VerifyConsumer(Consumer):
    """Verify Consumer class."""

    def handle_message(self, message: Message) -> None:
        """Handle message."""
        try:
            cmp_msg = json.loads(message.body)
            properties = {
                "content_type": "application/json",
                "headers": {},
                "correlation_id": message.correlation_id,
                "delivery_mode": 2,
            }

            # Create the message.
            channel = self.connection.channel()  # type: ignore
            accessionID = generate_accession_id()
            content = {
                "type": "accession",
                "user": cmp_msg["user"],
                "filepath": cmp_msg["filepath"],
                "decrypted_checksums": cmp_msg["decrypted_checksums"],
                "accession_id": accessionID,
            }
            accession = Message.create(channel, json.dumps(content), properties)
            checksum_data = list(filter(lambda x: x["type"] == "sha256", cmp_msg["decrypted_checksums"]))
            decrypted_checksum = checksum_data[0]["value"]
            accession.publish(
                os.environ.get("ACCESSIONIDS_QUEUE", "accessionIDs"), exchange=os.environ.get("BROKER_EXCHANGE", "sda")
            )

            channel.close()
            LOG.info(
                f'Sent the message to accessionIDs queue to set accession ID for file {cmp_msg["filepath"]} \
                     with checksum {decrypted_checksum}.'
            )

        except Exception as error:
            LOG.error("Something went wrong: {0}".format(error))


def main() -> None:
    """Run the Verify consumer."""
    CONSUMER = VerifyConsumer(
        hostname=str(os.environ.get("BROKER_HOST")),
        port=int(os.environ.get("BROKER_PORT", 5670)),
        username=os.environ.get("BROKER_USER", "sda"),
        password=os.environ.get("BROKER_PASSWORD", ""),
        queue=os.environ.get("VERIFIED_QUEUE", "verified"),
        vhost=os.environ.get("BROKER_VHOST", "sda"),
    )
    CONSUMER.start()


if __name__ == "__main__":
    main()
