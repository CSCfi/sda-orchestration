"""Message Broker verify step consumer."""
import json
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
from .utils.db_ops import map_file2dataset
import secrets
import string
import os
from time import sleep
from pathlib import Path


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
            stableID = "EGAF" + "".join(secrets.choice(string.digits) for i in range(16))
            content = {
                "user": cmp_msg["user"],
                "filepath": cmp_msg["filepath"],
                "decrypted_checksums": cmp_msg["decrypted_checksums"],
                "accession_id": stableID,
            }
            sent = Message.create(channel, json.dumps(content), properties)
            checksum_data = list(filter(lambda x: x["type"] == "sha256", cmp_msg["decrypted_checksums"]))
            decrypted_checksum = checksum_data[0]["value"]
            sent.publish(
                os.environ.get("STABLEIDS_QUEUE", "stableIDs"), exchange=os.environ.get("BROKER_EXCHANGE", "localega")
            )

            channel.close()
            LOG.info(
                f'Sent the message to files queue to trigger ingestion for file {cmp_msg["filepath"]} \
                     with checksum {decrypted_checksum}.'
            )

            sleep(20)
            file_path = Path(cmp_msg["filepath"])
            file_path_parts = file_path.parts
            dataset = ""
            # if a file it is submited in the root directory the dataset
            # is the urn:default:<username>
            # otherwise we take the root directory and construct the path
            # urn:dir:<root_dir>
            if len(file_path_parts) < 2:
                dataset = f'urn:default:{cmp_msg["user"]}'
            else:
                dataset = f"urn:dir:{file_path_parts[0]}"

            map_file2dataset(cmp_msg["user"], cmp_msg["filepath"], decrypted_checksum, dataset)

            LOG.info(f'filepath: {cmp_msg["decrypted_checksums"]} mapped stableID {stableID} and to dataset {dataset}.')

        except Exception as error:
            LOG.error("Something went wrong: {0}".format(error))


def main() -> None:
    """Run the Verify consumer."""
    CONSUMER = VerifyConsumer(
        hostname=str(os.environ.get("BROKER_HOST")),
        port=int(os.environ.get("BROKER_PORT", 5670)),
        username=os.environ.get("BROKER_USER", "lega"),
        password=os.environ.get("BROKER_PASSWORD"),
        queue=os.environ.get("VERIFIED_QUEUE", "files.verified"),
        vhost=os.environ.get("BROKER_VHOST", "lega"),
    )
    CONSUMER.start()


if __name__ == "__main__":
    main()
