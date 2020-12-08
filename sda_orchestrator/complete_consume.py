"""Message Broker complete step consumer."""
import json
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
import os
from .utils.id_ops import generate_dataset_id
from jsonschema.exceptions import ValidationError
from .schemas.validate import ValidateJSON, load_schema


class CompleteConsumer(Consumer):
    """Complete Consumer class."""

    def handle_message(self, message: Message) -> None:
        """Handle message."""
        try:
            complete_msg = json.loads(message.body)

            LOG.debug(f"MQ Message body: {message.body} .")
            LOG.debug(f"Complete Consumer message received: {complete_msg} .")
            LOG.info(
                f"Received work (corr-id: {message.correlation_id} filepath: {complete_msg['filepath']}, \
                user: {complete_msg['user']}, accessionid: {complete_msg['accession_id']}, \
                decryptedChecksums: {complete_msg['decrypted_checksums']})",
            )

            ValidateJSON(load_schema("ingestion-completion")).validate(complete_msg)

            # Send message to mappings queue for dataset to file mapping
            accessionID = complete_msg["accession_id"]
            datasetID = generate_dataset_id(complete_msg["user"], complete_msg["filepath"])
            self._publish_mappings(message, accessionID, datasetID)

        except ValidationError:
            LOG.error("Could not validate the ingestion complete message. Not properly formatted.")
            raise

        except Exception as error:
            LOG.error(f"Error occurred in complete consumer: {error}.")
            raise

    def _publish_mappings(self, message: Message, accessionID: str, datasetID: str) -> None:
        """Publish message with dataset to accession ID mapping."""
        properties = {
            "content_type": "application/json",
            "headers": {},
            "correlation_id": message.correlation_id,
            "delivery_mode": 2,
        }
        try:

            channel = self.connection.channel()  # type: ignore
            mappings_trigger = {"type": "mapping", "dataset_id": datasetID, "accession_ids": [accessionID]}

            mappings_msg = json.dumps(mappings_trigger)
            ValidateJSON(load_schema("dataset-mapping")).validate(json.loads(mappings_msg))

            mapping = Message.create(channel, mappings_msg, properties)
            mapping.publish(
                os.environ.get("MAPPINGS_QUEUE", "mappings"), exchange=os.environ.get("BROKER_EXCHANGE", "sda")
            )

            channel.close()

            LOG.info(
                f"Sent the message to mappings queue to set dataset ID {datasetID} for file \
                     with accessionID {accessionID}."
            )

        except ValidationError:
            LOG.error("Could not validate the ingestion mappings message. Not properly formatted.")
            raise Exception("Could not validate the ingestion mappings message. Not properly formatted.")


def main() -> None:
    """Run the Complete consumer."""
    CONSUMER = CompleteConsumer(
        hostname=str(os.environ.get("BROKER_HOST")),
        port=int(os.environ.get("BROKER_PORT", 5670)),
        username=os.environ.get("BROKER_USER", "sda"),
        password=os.environ.get("BROKER_PASSWORD", ""),
        queue=os.environ.get("COMPLETED_QUEUE", "completed"),
        vhost=os.environ.get("BROKER_VHOST", "sda"),
    )
    CONSUMER.start()


if __name__ == "__main__":
    main()
