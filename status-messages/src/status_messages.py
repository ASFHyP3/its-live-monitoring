"""Lambda function to trigger Mattermost updates for Dead Letter Queue."""

import argparse
import logging
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import boto3
from mattermostdriver import Driver


QUEUE_URL = os.environ['QUEUE_URL']
MATTERMOST_PAT = os.environ['MATTERMOST_PAT']

# You can find the ID for a channel by looking in the channel info
MATTERMOST_CHANNEL_ID = 'mmffdcqsafdg8xyr747scyuqnw'  # ~measures-its_live

log = logging.getLogger(__name__)
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def get_queue_count() -> str:
    """Retrieve the message count of the Dead Letter Queue.

    Returns:
        number_of_messages: count for Dead Letter Queue messages
    """
    client = boto3.client('sqs')
    result = client.get_queue_attributes(
        QueueUrl=QUEUE_URL,
        AttributeNames=['All'],
    )
    return result['Attributes']['ApproximateNumberOfMessages']


def lambda_handler(event: dict, context: dict) -> None:
    """Posts a message to Mattermost with the Dead Letter Queue count.

    Args:
        event: Lambda event
        context: Lambda context

    Returns:
        None
    """
    mattermost = Driver({'url': 'chat.asf.alaska.edu', 'token': MATTERMOST_PAT, 'scheme': 'https', 'port': 443})
    response = mattermost.login()
    log.debug(response)

    dead_letter_queue_count = int(get_queue_count())

    queue_name = Path(QUEUE_URL).name
    if 'test' in queue_name:
        status_emoji = ':heavy_multiplication_x:' if dead_letter_queue_count != 0 else ':heavy_check_mark:'
    else:
        status_emoji = ':alert:' if dead_letter_queue_count != 0 else ':large_green_circle:'

    mattermost_message = (
        f'{status_emoji} Dead Letter Queue Count for `{queue_name}` has '
        f'{dead_letter_queue_count} entries on {datetime.now(tz=UTC).isoformat()}'
    )

    log.info(f'Posting: "{mattermost_message}" to {MATTERMOST_CHANNEL_ID}')
    response = mattermost.posts.create_post(
        options={
            'channel_id': MATTERMOST_CHANNEL_ID,
            'message': mattermost_message,
        }
    )
    log.debug(response)


def main() -> None:
    """Command Line wrapper around `lambda_handler`."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-v', '--verbose', action='store_true', help='Turn on verbose logging')
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.debug(' '.join(sys.argv))
    lambda_handler({}, {})


if __name__ == '__main__':
    main()
