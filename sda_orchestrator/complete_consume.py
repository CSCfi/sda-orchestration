"""Message Broker complete step consumer."""
import json
from sda_orchestrator.utils.rems_ops import REMSHandler
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
from os import environ
from .utils.id_ops import generate_dataset_id, DOIHandler
from jsonschema.exceptions import ValidationError
from .schemas.validate import ValidateJSON, load_schema
import asyncio


class CompleteConsumer(Consumer):
    """Complete Consumer class."""

    def handle_message(self, message: Message) -> None:
        """Handle message."""
        try:
            complete_msg = json.loads(message.body)

            LOG.debug(f"MQ Message body: {message.body} .")
            LOG.debug(f"Complete Consumer message received: {complete_msg} .")
            LOG.info(
                f"Received work (corr-id: {message.correlation_id} filepath: {complete_msg['filepath']},"
                f"user: {complete_msg['user']}, accessionid: {complete_msg['accession_id']},"
                f"decryptedChecksums: {complete_msg['decrypted_checksums']})"
            )

            ValidateJSON(load_schema("ingestion-completion")).validate(complete_msg)

            # Send message to mappings queue for dataset to file mapping
            accessionID = complete_msg["accession_id"]
            datasetID = asyncio.run(self._process_datasetID(complete_msg["user"], complete_msg["filepath"]))
            self._publish_mappings(message, accessionID, datasetID)

        except ValidationError:
            LOG.error("Could not validate the ingestion complete message. Not properly formatted.")
            raise

        except Exception as error:
            LOG.error(f"Error occurred in complete consumer: {error}.")
            raise

    async def _process_datasetID(self, user: str, filepath: str) -> str:
        """Process and generated dataset ID depending on environment variable set.

        If we make use of Datacite and REMS we need to check if env vars are set.
        First we create a draft DOI then we register in REMS after which we publish
        the DOI.
        """
        datasetID: str = ""
        try:
            if (
                "DOI_PREFIX" in environ and "DOI_API" in environ and "DOI_USER" in environ and "DOI_KEY" in environ
            ) and ("REMS_API" in environ and "REMS_USER" in environ and "REMS_KEY" in environ):
                doi_handler = DOIHandler()
                rems = REMSHandler()
                doi_obj = await doi_handler.create_draft_doi(user, filepath)
                LOG.info(f"Registered dataset {doi_obj}.")
                if doi_obj:
                    await rems.register_resource(doi_obj["fullDOI"])
                else:
                    LOG.error("Registering a DOI was not possible.")
                    raise Exception("Registering a DOI was not possible.")

                datasetID = doi_obj["fullDOI"]
                await doi_handler.set_doi_state("publish", doi_obj["suffix"])
            else:
                datasetID = generate_dataset_id(user, filepath)
        except Exception as error:
            LOG.error(f"Could not process datasetID because of: {error}.")
            raise
        else:
            return datasetID

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
            mapping.publish(environ.get("MAPPINGS_QUEUE", "mappings"), exchange=environ.get("BROKER_EXCHANGE", "sda"))

            channel.close()

            LOG.info(
                f"Sent the message to mappings queue to set dataset ID {datasetID} for file"
                f"with accessionID {accessionID}."
            )

        except ValidationError:
            LOG.error("Could not validate the ingestion mappings message. Not properly formatted.")
            raise Exception("Could not validate the ingestion mappings message. Not properly formatted.")


def main() -> None:
    """Run the Complete consumer."""
    CONSUMER = CompleteConsumer(
        hostname=str(environ.get("BROKER_HOST")),
        port=int(environ.get("BROKER_PORT", 5670)),
        username=environ.get("BROKER_USER", "sda"),
        password=environ.get("BROKER_PASSWORD", ""),
        queue=environ.get("COMPLETED_QUEUE", "completed"),
        vhost=environ.get("BROKER_VHOST", "sda"),
    )
    CONSUMER.start()


if __name__ == "__main__":
    main()
