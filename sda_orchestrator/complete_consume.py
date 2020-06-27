import json
from .utils.consumer import Consumer
from .utils.logger import LOG
import os


class CompleteConsumer(Consumer):
    """."""

    def handle_message(self, message):
        """Handle message."""
        try:
            cmp_msg = json.loads(message.body)

            LOG.info(f'completed message recived: {cmp_msg} .')

        except Exception as error:
            LOG.error('Something went wrong: {0}'.format(error))


def main():
    """Run the Complete consumer."""
    CONSUMER = CompleteConsumer(hostname=str(os.environ.get('BROKER_HOST')),
                                port=int(os.environ.get('BROKER_PORT', 5670)),
                                username=os.environ.get('BROKER_USER', 'lega'),
                                password=os.environ.get('BROKER_PASSWORD'),
                                queue='v1.files.completed',
                                vhost=os.environ.get('BROKER_VHOST', 'lega'))
    CONSUMER.start()


if __name__ == '__main__':
    main()
