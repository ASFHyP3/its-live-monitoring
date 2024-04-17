import datetime
import unittest.mock
import json

import geopandas as gpd  # For a TODO
import hyp3_sdk as sdk  # For a TODO
import pystac

from unittest.mock import MagicMock  # For a TODO
from dateutil.tz import tzutc

from sentinel2.src import main
import pdb

SENTINEL2_CATALOG_real = main.SENTINEL2_CATALOG
HYP3_real = main.HYP3

# TODO: Make a version of `tests/data/scene1_pair.parquet` for Sentinel-2
SAMPLE_PAIRS = gpd.read_parquet('tests/data/sentinel2/S2B_13CES_20200315_0_L1C_pairs.parquet')


def get_mock_pystac_item() -> unittest.mock.NonCallableMagicMock:
    item = unittest.mock.NonCallableMagicMock()
    item.collection_id = 'sentinel-2-l1c'
    item.properties = {
        'instruments': ['msi'],
        'mgrs:utm_zone': '19',
        'mgrs:latitude_band': 'D',
        'mgrs:grid_square': 'EE',
        'eo:cloud_cover': 30,
    }
    return item


def test_qualifies_for_processing():
    item = get_mock_pystac_item()
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.collection_id = 'foo'
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['instruments'] = ['mis']
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['mgrs:utm_zone'] = '30'
    item.properties['mgrs:latitude_band'] = 'B'
    item.properties['mgrs:grid_square'] = 'ZZ'
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['eo:cloud_cover'] = 75
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['eo:cloud_cover'] = 0
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['eo:cloud_cover'] = -1
    assert not main._qualifies_for_processing(item)


def get_expected_item1():
    f1=open('tests/data/sentinel2/S2B_13CES_20200315_0_L1C_scene.json', 'r')
    item_dict = json.load(f1)
    f1.close()
    item = pystac.item.Item.from_dict(item_dict)
    return item


def get_expected_item():
    scene = 'S2B_19DEE_20231129_0_L1C'
    expected_datetime = datetime.datetime(2023, 11, 29, 13, 21, 40, 694000, tzinfo=tzutc())
    expected_collection_id = 'sentinel-2-l1c'
    expected_properties = {
        'created': '2023-11-29T18:11:44.670Z',
        'instruments': ['msi'],
        'eo:cloud_cover': 42.9271415094498,
        'mgrs:utm_zone': 19,
        'mgrs:latitude_band': 'D',
        'mgrs:grid_square': 'EE',
    }
    expected_item = pystac.item.Item(
        id=scene,
        geometry=None,
        bbox=None,
        datetime=expected_datetime,
        properties=expected_properties,
        collection=expected_collection_id,
    )
    return expected_item


def test_get_stac_item():
    scene = 'S2B_19DEE_20231129_0_L1C'
    expect_item = get_expected_item()

    item = main._get_stac_item(scene)

    assert item.collection_id == expect_item.collection_id
    assert item.properties['instruments'] == expect_item.properties['instruments']
    assert item.properties['created'] == expect_item.properties['created']
    assert item.properties['mgrs:utm_zone'] == expect_item.properties['mgrs:utm_zone']
    assert item.properties['mgrs:latitude_band'] == expect_item.properties['mgrs:latitude_band']
    assert item.properties['mgrs:grid_square'] == expect_item.properties['mgrs:grid_square']
    assert item.properties['eo:cloud_cover'] == expect_item.properties['eo:cloud_cover']


# TODO:  Make a version of `tests/data/scene1_return_itemcollection.json` for Sentinel-2

def get_expected_jobs():
    job1 = sdk.jobs.Job.from_dict(
         {'job_id': 'f95a5921-0987-46fe-a43b-1cd4bd07cc02', 'job_type': 'AUTORIFT',
         'request_time': '2024-04-16T00:27:24+00:00', 'status_code': 'FAILED',
         'user_id': 'cirrusasf', 'name': 'jz_s2_t1',
         'job_parameters': {'granules':
         ['S2B_MSIL1C_20200315T152259_N0209_R039_T13CES_20200315T181115',
         'S2B_MSIL1C_20190211T142249_N0207_R067_T13CES_20190211T185312']},
         'logs': ['https://d1riv60tezqha9.cloudfront.net/f95a5921-0987-46fe-a43b-1cd4bd07cc02/f95a5921-0987-46fe-a43b-1cd4bd07cc02.log'],
         'expiration_time': '2024-05-01T00:00:00+00:00', 'processing_times': [105.39], 'credit_cost': 25}
    )
    job2 = sdk.jobs.Job.from_dict(
        {'job_id': 'a3fea943-ad61-4a45-a05f-b19ce4a817ef', 'job_type': 'AUTORIFT',
        'request_time': '2024-04-16T00:28:32+00:00', 'status_code': 'FAILED',
        'user_id': 'cirrusasf', 'name': 'jz_s2_t2',
        'job_parameters': {'granules':
        ['S2B_MSIL1C_20200315T152259_N0209_R039_T13CES_20200315T181115',
        'S2B_MSIL1C_20200201T151249_N0500_R139_T13CES_20230425T114956']},
        'logs': ['https://d1riv60tezqha9.cloudfront.net/a3fea943-ad61-4a45-a05f-b19ce4a817ef/a3fea943-ad61-4a45-a05f-b19ce4a817ef.log'],
        'expiration_time': '2024-05-01T00:00:00+00:00', 'processing_times': [2.076], 'credit_cost': 25}
    )

    jobs_expected = sdk.jobs.Batch([job1, job2])
    return jobs_expected


def test_get_landsat_pairs_for_reference_scene():
    pdb.set_trace()
    main.SENTINEL2_CATALOG = MagicMock()
    reference_item = get_expected_item1()
    with open('tests/data/sentinel2/S2B_13CES_20200315_0_L1C_pages.json', 'r') as f:
        pages_dict = json.load(f)
        pages = (pystac.item_collection.ItemCollection.from_dict(page) for page in pages_dict)

    main.SENTINEL2_CATALOG.search().pages.return_value = pages

    df = main.get_sentinel2_pairs_for_reference_scene(reference_item)

    assert (df['mgrs:utm_zone'] == reference_item.properties['mgrs:utm_zone']).all()
    assert (df['mgrs:latitude_band'] == reference_item.properties['mgrs:latitude_band']).all()
    assert (df['mgrs:grid_square'] == reference_item.properties['mgrs:grid_square']).all()
    assert (df['instruments'].apply(lambda x: ''.join(x)) == ''.join(reference_item.properties['instruments'])).all()
    assert (df['reference'] == reference_item.id).all()


def test_deduplicate_hyp3_pairs(pairs=SAMPLE_PAIRS):
    # pdb_set_trace()
    duplicate_jobs = get_expected_jobs()

    main.HYP3 = MagicMock()
    main.HYP3.find_jobs.return_value = duplicate_jobs

    new_pairs = main.deduplicate_hyp3_pairs(pairs)
    main.HYP3 = HYP3_real

    p_idx = pairs.set_index(['reference', 'secondary'])
    np_idx = new_pairs.set_index(['reference', 'secondary'])
    assert np_idx.isin(p_idx).any().any()
    assert len(p_idx) - 2 == len(np_idx)


def test_submit_pairs_for_processing(pairs=SAMPLE_PAIRS):
    jobs_expect = get_expected_jobs()

    main.HYP3.submit_prepared_jobs = MagicMock()
    main.HYP3.submit_prepared_jobs.return_value = jobs_expect

    jobs = main.submit_pairs_for_processing(pairs)
    main.HYP3 = HYP3_real

    assert jobs == jobs_expect
