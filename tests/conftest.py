import datetime as dt
from copy import deepcopy
from unittest.mock import NonCallableMock

import boto3
import hyp3_sdk as sdk
import pystac
import pytest
from dateutil.parser import parse as date_parser
from moto import mock_aws


@pytest.fixture
def pystac_item_factory():
    def create_pystac_item(
        id: str,
        datetime: str | dt.datetime,
        properties: dict,
        collection: str,
        geometry: dict | None = None,
        bbox: list | None = None,
        assets: dict | None = None,
    ) -> pystac.item.Item:
        if isinstance(datetime, str):
            datetime = date_parser(datetime)

        expected_item = pystac.item.Item(
            id=id,
            geometry=geometry if geometry is None else deepcopy(geometry),
            bbox=bbox,
            datetime=datetime,
            properties=deepcopy(properties),
            collection=collection,
            assets=assets if assets is None else deepcopy(assets),
        )

        return expected_item

    return create_pystac_item


@pytest.fixture
def stac_search_factory():
    class MockItemSearch:
        def __init__(self, items: list[pystac.item.Item]):
            self.items = items

        def pages(self):
            return [self.items]

    return MockItemSearch


@pytest.fixture
def hyp3_job_factory():
    def create_hyp3_job(granules: list) -> sdk.Job:
        return NonCallableMock(job_parameters={'granules': granules})

    return create_hyp3_job


@pytest.fixture
def hyp3_batch_factory(hyp3_job_factory):
    def create_hyp3_batch(granules_list: list) -> sdk.Batch:
        return sdk.Batch([hyp3_job_factory(granules) for granules in granules_list])

    return create_hyp3_batch


@mock_aws
@pytest.fixture
def tables():
    table_properties = {
        'BillingMode': 'PAY_PER_REQUEST',
        'AttributeDefinitions': [
            {'AttributeName': 'job_id', 'AttributeType': 'S'},
            {'AttributeName': 'user_id', 'AttributeType': 'S'},
            {'AttributeName': 'status_code', 'AttributeType': 'S'},
            {'AttributeName': 'request_time', 'AttributeType': 'S'},
        ],
        'KeySchema': [{'AttributeName': 'job_id', 'KeyType': 'HASH'}],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'user_id',
                'KeySchema': [
                    {'AttributeName': 'user_id', 'KeyType': 'HASH'},
                    {'AttributeName': 'request_time', 'KeyType': 'RANGE'},
                ],
                'Projection': {'ProjectionType': 'ALL'},
            },
            {
                'IndexName': 'status_code',
                'KeySchema': [{'AttributeName': 'status_code', 'KeyType': 'HASH'}],
                'Projection': {'ProjectionType': 'ALL'},
            },
        ],
    }

    with mock_aws():
        dynamo = boto3.resource('dynamodb')

        class Tables:
            jobs_table = dynamo.create_table(
                TableName=environ['JOBS_TABLE_NAME'],
                **table_properties,
            )

        tables = Tables()
        yield tables
