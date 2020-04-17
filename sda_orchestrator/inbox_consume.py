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
            content = {"user": inbx_msg["user"], "filepath": inbx_msg["filepath"].replace(f'/ega/inbox/{inbx_msg["user"]}', ''),
                       "encrypted_checksum": inbx_msg["encrypted_checksum"]}
            sent = Message.create(channel, json.dumps(content), properties)

            sent.publish('files', exchange='localega.v1')
            channel.close()
            LOG.info(f'Sent the message to files queue to trigger ingestion for filepath: {inbx_msg["filepath"]}.')
        except Exception as error:
            LOG.error('Something went wrong: {0}'.format(error))


if __name__ == '__main__':
    CONSUMER = InboxConsumer(port=os.environ.get('MQ_PORT', 5670),
                             username=os.environ.get('MQ_USER', 'lega'),
                             password=os.environ.get('MQ_PASSWORD'),
                             queue='v1.files.completed',
                             vhost=os.environ.get('MQ_VHOST', 'lega'))
    CONSUMER.start()
