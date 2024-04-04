"""Lambda function to trigger Mattermost updates for Dead Letter Queue."""

import argparse
import logging
import os
import sys
from datetime import datetime

import boto3
from mattermostdriver import Driver


def get_queue_status(queue_url: str) -> str:
    """Retrieve the status of the Dead Letter Queue for URL.

    Args:
        queue_url: Url for

    Returns:
        number_of_messages: count for Dead Letter Queue messages
    """
    client = boto3.client('sqs')
    result = client.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['All'],
    )
    return result['Attributes']['ApproximateNumberOfMessages']


def post(channel: str = 'measures-its_live') -> dict:
    """Posts a message to Mattermost with the Dead Letter Queue count for specified deployment.

    Args:
        channel: Mattermost channel to post to

    Returns:
        response: from Mattermost
    """
    mattermost = Driver(
        {'url': 'chat.asf.alaska.edu', 'token': os.environ.get('MATTERMOST_PAT'), 'scheme': 'https',
         'port': 443}
    )
    response = mattermost.login()
    logging.debug(response)

    channel_info = mattermost.channels.get_channel_by_name_and_team_name('asf', channel)

    queue_url = 'https://sqs.us-west-2.amazonaws.com/986442313181/its-live-monitoring-prod-DeadLetterQueue-LjzW63l95LAP'
    dead_letter_queue_count = get_queue_status(queue_url)
    mattermost_message = (
        f'Dead Letter Queue Count for ITSLIVE has '
        f'{dead_letter_queue_count} entries on {datetime.now().strftime("%m/%d/%Y")}'
        )
    response = mattermost.posts.create_post(
        options={
            'channel_id': channel_info['id'],
            'message': mattermost_message,
        }
    )
    logging.debug(response)

    return response


def main() -> None:
    """Command Line wrapper around `post`."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('--channel', default='measures-its_live', help='The MatterMost channel to post to')

    args = parser.parse_args()

    out = logging.StreamHandler(stream=sys.stdout)
    out.addFilter(lambda record: record.levelno <= logging.INFO)
    err = logging.StreamHandler()
    err.setLevel(logging.WARNING)
    logging.basicConfig(format='%(message)s', level=logging.INFO, handlers=(out, err))

    post(**args.__dict__)


if __name__ == '__main__':
    main()
