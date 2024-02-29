import json


def process_scene(scene):
    print(scene)


def lambda_handler(event, context):
    for record in event['Records']:
        body = json.loads(record['body'])
        message = json.loads(body['Message'])
        process_scene(message['landsat_product_id'])
