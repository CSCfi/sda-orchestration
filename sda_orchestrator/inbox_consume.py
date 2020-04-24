import json
from amqpstorm import Message
from .utils.consumer import Consumer
from .utils.logger import LOG
import os


class InboxConsumer(Consumer):
    """."""

    def handle_message(self, message):
        """Handle message."""
        try:
            inbx_msg = json.loads(message.body)
            properties = {
                'content_type': 'application/json',
                'headers': {},
                'correlation_id': message.correlation_id
            }

            # Create the files message.
            # we keep the encrypted_checksum but it can also be missing
            channel = self.connection.channel()
            content = {"user": inbx_msg["user"], "filepath": inbx_msg["filepath"].replace(f'/ega/inbox/{inbx_msg["user"]}', '')}
            if "encrypted_checksums" in inbx_msg:
                content["encrypted_checksums"] = inbx_msg["encrypted_checksums"]
            sent = Message.create(channel, json.dumps(content), properties)

            sent.publish('files', exchange='localega.v1')
            channel.close()
            LOG.info(f'Sent the message to files queue to trigger ingestion for filepath: {inbx_msg["filepath"]}.')
        except Exception as error:
            LOG.error('Something went wrong: {0}'.format(error))


def main():
    """Run the Inbox consumer."""
    CONSUMER = InboxConsumer(hostname=str(os.environ.get('BROKER_HOST')),
                             port=int(os.environ.get('BROKER_PORT', 5670)),
                             username=os.environ.get('BROKER_USER', 'lega'),
                             password=os.environ.get('BROKER_PASSWORD'),
                             queue='v1.files.inbox',
                             vhost=os.environ.get('BROKER_VHOST', 'lega'))
    CONSUMER.start()


if __name__ == '__main__':
    main()