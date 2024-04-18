import datetime as dt
from unittest.mock import NonCallableMock

import hyp3_sdk as sdk
import pystac
import pytest


@pytest.fixture
def pystac_item_factory():
    def create_pystac_item(
        id: str,
        datetime: dt.datetime,
        properties: dict,
        collection: str,
    ) -> pystac.item.Item:
        expected_item = pystac.item.Item(
            id=id,
            geometry=None,
            bbox=None,
            datetime=datetime,
            properties=properties,
            collection=collection,
        )
        return expected_item

    return create_pystac_item


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
