import datetime
import unittest.mock

# import geopandas as gpd  # For a TODO
# import hyp3_sdk as sdk  # For a TODO
import pystac

# from unittest.mock import MagicMock  # For a TODO
from dateutil.tz import tzutc

from sentinel2.src import main


SENTINEL2_CATALOG_real = main.SENTINEL2_CATALOG
HYP3_real = main.HYP3

# TODO: Make a version of `tests/data/scene1_pair.parquet` for Sentinel-2
# SAMPLE_PAIRS = gpd.read_parquet('tests/data/scene1_pair.parquet')


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

# def get_expected_jobs():
#     job1 = sdk.jobs.Job.from_dict(
#         {
#             'job_id': '88ea6109-8afa-483a-93d5-7f3231db7751',
#             'job_type': 'AUTORIFT',
#             'request_time': '2024-04-09T18:13:41+00:00',
#             'status_code': 'PENDING',
#             'user_id': 'cirrusasf',
#             'name': 'LC08_L1TP_138041_20240128_20240207_02_T1',
#             'job_parameters': {
#                 'granules': ['LC08_L1TP_138041_20240128_20240207_02_T1', 'LC09_L1TP_138041_20240120_20240120_02_T1'],
#                 'parameter_file': '/vsicurl/http://its-live-data.s3.amazonaws.com/'
#                 'autorift_parameters/v001/autorift_landice_0120m.shp',
#                 'publish_bucket': '""',
#             },
#             'credit_cost': 1,
#         }
#     )
#     job2 = sdk.jobs.Job.from_dict(
#         {
#             'job_id': '4eea15af-167a-43b3-b292-aee55b3e893e',
#             'job_type': 'AUTORIFT',
#             'request_time': '2024-04-09T18:15:06+00:00',
#             'status_code': 'PENDING',
#             'user_id': 'cirrusasf',
#             'name': 'LC08_L1TP_138041_20240128_20240207_02_T1',
#             'job_parameters': {
#                 'granules': ['LC08_L1TP_138041_20240128_20240207_02_T1', 'LC08_L1TP_138041_20231227_20240104_02_T1'],
#                 'parameter_file': '/vsicurl/http://its-live-data.s3.amazonaws.com/'
#                 'autorift_parameters/v001/autorift_landice_0120m.shp',
#                 'publish_bucket': '""',
#             },
#             'credit_cost': 1,
#         }
#     )

#     jobs_expected = sdk.jobs.Batch([job1, job2])
#     return jobs_expected


# def test_get_landsat_pairs_for_reference_scene():
#     main.SENTINEL2_CATALOG = MagicMock()
#     reference_item = get_expected_item()
#     results_item_collection = pystac.item_collection.ItemCollection.from_file(
#         'tests/data/scene1_return_itemcollection.json'
#     )
#     data_gen = (y for y in [results_item_collection])
#     main.LANDSAT_CATALOG.search().pages.return_value = data_gen

#     df = main.get_landsat_pairs_for_reference_scene(reference_item)

#     assert (df['mgrs:utm_zone'] == reference_item.properties['mgrs:utm_zone']).all()
#     assert (df['mgrs:latitude_band'] == reference_item.properties['mgrs:latitude_band']).all()
#     assert (df['mgrs:grid_square'] == reference_item.properties['mgrs:grid_square']).all()
#     assert (df['instruments'].apply(lambda x: ''.join(x)) == ''.join(reference_item.properties['instruments'])).all()
#     assert (df['reference'] == reference_item.id).all()


# def test_deduplicate_hyp3_pairs(pairs=SAMPLE_PAIRS):
#     duplicate_jobs = get_expected_jobs()

#     main.HYP3 = MagicMock()
#     main.HYP3.find_jobs.return_value = duplicate_jobs

#     new_pairs = main.deduplicate_hyp3_pairs(pairs)
#     main.HYP3 = HYP3_real

#     p_idx = pairs.set_index(['reference', 'secondary'])
#     np_idx = new_pairs.set_index(['reference', 'secondary'])
#     assert np_idx.isin(p_idx).any().any()
#     assert len(p_idx) - 2 == len(np_idx)


# def test_submit_pairs_for_processing(pairs=SAMPLE_PAIRS):
#     jobs_expect = get_expected_jobs()

#     main.HYP3.submit_prepared_jobs = MagicMock()
#     main.HYP3.submit_prepared_jobs.return_value = jobs_expect

#     jobs = main.submit_pairs_for_processing(pairs)
#     main.HYP3 = HYP3_real

#     assert jobs == jobs_expect
