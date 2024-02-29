"""Lambda function to trigger low-latency Landsat processing from newly acquired scenes."""

import json


def process_scene(scene: str) -> None:
    """Process a Landsat scene."""
    print(scene)


def lambda_handler(event: dict, context: object) -> None:
    """Landsat processing lambda function.

    Accepts an event with SQS records for newly ingested Landsat scenes and processes each scene.

    Args:
        event: The event dictionary that contains the parameters sent when this function is invoked.
        context: The context in which is function is called.
    """
    for record in event['Records']:
        body = json.loads(record['body'])
        message = json.loads(body['Message'])
        process_scene(message['landsat_product_id'])
