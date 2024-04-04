"""Lambda function to trigger Mattermost updates for Dead Letter Queue."""

import os
from datetime import datetime

import boto3
from mattermostdriver import Driver


CHANNEL = 'measures-its_live'
QUEUE_URL = os.environ['QUEUE_URL']
MATTERMOST_PAT = os.environ['MATTERMOST_PAT']


def get_queue_count() -> str:
    """Retrieve the status of the Dead Letter Queue for URL.

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
        response: from Mattermost
    """
    mattermost = Driver(
        {'url': 'chat.asf.alaska.edu', 'token': MATTERMOST_PAT, 'scheme': 'https', 'port': 443}
    )
    response = mattermost.login()
    print(response)

    channel_info = mattermost.channels.get_channel_by_name_and_team_name('asf', CHANNEL)
    dead_letter_queue_count = get_queue_count()
    mattermost_message = (
        f'Dead Letter Queue Count for ITS_LIVE has '
        f'{dead_letter_queue_count} entries on {datetime.now().strftime("%m/%d/%Y")}'
    )
    response = mattermost.posts.create_post(
        options={
            'channel_id': channel_info['id'],
            'message': mattermost_message,
        }
    )
    print(response)
