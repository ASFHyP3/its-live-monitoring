import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
from mattermostdriver import Driver


def get_itslive_queue():
    os.environ['AWS_PROFILE'] = 'hyp3-its-live'
    client = boto3.client('sqs')
    result = client.get_queue_attributes(
        QueueUrl='https://sqs.us-west-2.amazonaws.com/986442313181/its-live-monitoring-prod-DeadLetterQueue'
                 '-LjzW63l95LAP',
        AttributeNames=['All'],
    )
    return result['Attributes']['ApproximateNumberOfMessages']


def post(markdown_file: Path, channel: str = 'APD'):
    mattermost = Driver({
        'url': 'chat.asf.alaska.edu',
        'token': os.environ.get('MATTERMOST_PAT'),
        'scheme': 'https',
        'port': 443
    })
    response = mattermost.login()
    logging.debug(response)

    channel_info = mattermost.channels.get_channel_by_name_and_team_name('asf', channel)

    dead_letter_queue_count = get_itslive_queue()
    mattermost_message = (f'Dead Letter Queue Count as of {datetime.now()} has '
                          f'{dead_letter_queue_count} entries')

    response = mattermost.posts.create_post(
        options={
            'channel_id': channel_info['id'],
            'message': mattermost_message,
        }
    )
    logging.debug(response)

    return response


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('markdown_file', type=Path, help='Markdown file with the post content')
    parser.add_argument('--channel', default='tools-team', help='The MatterMost channel to post to')

    args = parser.parse_args()

    out = logging.StreamHandler(stream=sys.stdout)
    out.addFilter(lambda record: record.levelno <= logging.INFO)
    err = logging.StreamHandler()
    err.setLevel(logging.WARNING)
    logging.basicConfig(format='%(message)s', level=logging.INFO, handlers=(out, err))

    post(**args.__dict__)
