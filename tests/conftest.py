import datetime as dt
import json
from copy import deepcopy
from os import environ
from pathlib import Path
from unittest.mock import NonCallableMock

import boto3
import hyp3_sdk as sdk
import pystac
import pytest
import pytz
from asf_search.ASFProduct import ASFProduct
from asf_search.ASFSearchResults import ASFSearchResults
from dateutil.parser import parse as date_parser
from moto import mock_aws

from sentinel1 import BURST_IDS_TO_OPERA_FRAMES, OPERA_FRAMES_TO_BURST_IDS


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
def asf_product_factory():
    def create_asf_product(
            scene_name: str,
            full_burst_id: str,
            polarization: str,
            start_time: str | dt.datetime,
    ) -> ASFProduct:
        if isinstance(start_time, str):
            start_time = date_parser(start_time)

        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=dt.UTC)


        start_time = start_time.isoformat(timespec='seconds')

        product = ASFProduct()
        product.properties.update({
            'sceneName': scene_name,
            'startTime': start_time,
            'polarization': polarization,
            'burst': {
                'fullBurstID': full_burst_id
            }
        })
        return deepcopy(product)

    return create_asf_product


@pytest.fixture
def asf_stack_factory(asf_product_factory):
    def create_asf_burst_stacks(
            scene_name: str,
            full_burst_id: str,  # track is not easily derived from scene name
            days_seperation: range = range(0, 13, 6),
    ) -> list[ASFSearchResults]:
        _, _, _, start_time, polarization, _ = scene_name.split('_')
        start_times = [date_parser(start_time) - dt.timedelta(days=ii) for ii in days_seperation]
        scene_names = [scene_name.replace(start_time, st.strftime('%Y%m%dT%H%M%S')) for st in start_times]
        frame_ids = BURST_IDS_TO_OPERA_FRAMES[full_burst_id]

        stacks = []
        for frame_id in frame_ids:
            burst_ids = OPERA_FRAMES_TO_BURST_IDS[str(frame_id)]
            for burst_id in burst_ids:
                stack = ASFSearchResults()
                stack.data = [
                    asf_product_factory(sn, burst_id, polarization, st)
                    for sn, st in zip(scene_names, start_times)
                ]
                stacks.append(stack)

        return stacks

    return create_asf_burst_stacks


@pytest.fixture
def hyp3_job_factory():
    def create_hyp3_job(granules: list) -> sdk.Job:
        return NonCallableMock(job_parameters={'reference': granules[0], 'secondary': granules[1]})

    return create_hyp3_job


@pytest.fixture
def hyp3_batch_factory(hyp3_job_factory):
    def create_hyp3_batch(granules_list: list) -> sdk.Batch:
        return sdk.Batch([hyp3_job_factory(granules) for granules in granules_list])

    return create_hyp3_batch


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


@pytest.fixture(scope='session')
def landsat_message():
    example = Path(__file__).parent / 'integration' / 'landsat-l8-valid.json'
    return json.loads(example.read_text())


@pytest.fixture(scope='session')
def sentinel1_burst_message():
    example = Path(__file__).parent / 'integration' / 'sentinel1-burst-valid.json'
    return json.loads(example.read_text())


@pytest.fixture(scope='session')
def sentinel2_message():
    example = Path(__file__).parent / 'integration' / 'sentinel2-valid.json'
    return json.loads(example.read_text())
