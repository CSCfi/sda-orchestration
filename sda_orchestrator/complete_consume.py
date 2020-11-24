"""Message Broker complete step consumer."""
import json
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
import os
from .utils.id_ops import generate_dataset_id


class CompleteConsumer(Consumer):
    """Complete Consumer class."""

    def handle_message(self, message: Message) -> None:
        """Handle message."""
        try:
            complete_msg = json.loads(message.body)

            LOG.info(f"Completed message received: {complete_msg} .")
            properties = {
                "content_type": "application/json",
                "headers": {},
                "correlation_id": message.correlation_id,
                "delivery_mode": 2,
            }

            channel = self.connection.channel()  # type: ignore
            datasetID = generate_dataset_id(complete_msg["user"], complete_msg["filepath"])
            accessionID = complete_msg["accession_id"]
            content = {
                "type": "mapping",
                "dataset_id": datasetID,
                "accession_ids": [accessionID],
            }
            mapping = Message.create(channel, json.dumps(content), properties)
            mapping.publish(
                os.environ.get("MAPPINGS_QUEUE", "mappings"), exchange=os.environ.get("BROKER_EXCHANGE", "sda")
            )

            channel.close()
            LOG.info(
                f'Sent the message to mappings queue to set dataset ID {datasetID} for file \
                     with accessionID {accessionID}.'
            )

        except Exception as error:
            LOG.error("Something went wrong: {0}".format(error))


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
