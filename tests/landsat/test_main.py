import datetime
import unittest.mock
from unittest.mock import MagicMock

import geopandas as gpd
import hyp3_sdk as sdk
import pystac

from landsat.src import main


LANDSAT_CATALOG_real = main.LANDSAT_CATALOG
HYP3_real = main.HYP3
SAMPLE_PAIRS = gpd.read_parquet('tests/data/scene1_pair.parquet')


def get_mock_pystac_item() -> unittest.mock.NonCallableMagicMock:
    item = unittest.mock.NonCallableMagicMock()
    item.collection_id = 'landsat-c2l1'
    item.properties = {
        'instruments': ['OLI'],
        'landsat:collection_category': 'T1',
        'landsat:wrs_path': '001',
        'landsat:wrs_row': '005',
        'landsat:cloud_cover_land': 50,
        'view:off_nadir': 0,
    }
    return item


def test_qualifies_for_processing():
    item = get_mock_pystac_item()
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.collection_id = 'foo'
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['instruments'] = ['TIRS']
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:collection_category'] = 'T2'
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:collection_category'] = 'RT'
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:wrs_path'] = 'foo'
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:wrs_row'] = 'foo'
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    del item.properties['landsat:cloud_cover_land']
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:cloud_cover_land'] = -1
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:cloud_cover_land'] = 0
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:cloud_cover_land'] = 1
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:cloud_cover_land'] = main.MAX_CLOUD_COVER_PERCENT - 1
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:cloud_cover_land'] = main.MAX_CLOUD_COVER_PERCENT
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['landsat:cloud_cover_land'] = main.MAX_CLOUD_COVER_PERCENT + 1
    assert not main._qualifies_for_processing(item)


def get_expected_item():
    scene = 'LC08_L1TP_138041_20240128_20240207_02_T1'
    expected_datetime = datetime.datetime(2024, 1, 28, 4, 29, 49, 361022)
    expected_datetime = expected_datetime.replace(tzinfo=datetime.timezone.utc)
    expected_collection_id = 'landsat-c2l1'
    expected_properties = {
        'datetime': '2024-01-28T04:29:49.361022Z',
        'eo:cloud_cover': 11.59,
        'view:sun_azimuth': 148.43311105,
        'view:sun_elevation': 37.83753177,
        'platform': 'LANDSAT_8',
        'instruments': ['OLI', 'TIRS'],
        'view:off_nadir': 0,
        'landsat:cloud_cover_land': 11.59,
        'landsat:wrs_type': '2',
        'landsat:wrs_path': '138',
        'landsat:wrs_row': '041',
        'landsat:scene_id': 'LC81380412024028LGN00',
        'landsat:collection_category': 'T1',
        'landsat:collection_number': '02',
        'landsat:correction': 'L1TP',
        'accuracy:geometric_x_bias': 0,
        'accuracy:geometric_y_bias': 0,
        'accuracy:geometric_x_stddev': 3.926,
        'accuracy:geometric_y_stddev': 3.525,
        'accuracy:geometric_rmse': 5.277,
        'proj:epsg': 32645,
        'proj:shape': [7681, 7531],
        'proj:transform': [30, 0, 674985, 0, -30, 3152415],
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


def get_expected_jobs():
    job1 = sdk.jobs.Job.from_dict(
        {
            'job_id': '88ea6109-8afa-483a-93d5-7f3231db7751',
            'job_type': 'AUTORIFT',
            'request_time': '2024-04-09T18:13:41+00:00',
            'status_code': 'PENDING',
            'user_id': 'cirrusasf',
            'name': 'LC08_L1TP_138041_20240128_20240207_02_T1',
            'job_parameters': {
                'granules': ['LC08_L1TP_138041_20240128_20240207_02_T1', 'LC09_L1TP_138041_20240120_20240120_02_T1'],
                'parameter_file': '/vsicurl/http://its-live-data.s3.amazonaws.com/'
                'autorift_parameters/v001/autorift_landice_0120m.shp',
                'publish_bucket': '""',
            },
            'credit_cost': 1,
        }
    )
    job2 = sdk.jobs.Job.from_dict(
        {
            'job_id': '4eea15af-167a-43b3-b292-aee55b3e893e',
            'job_type': 'AUTORIFT',
            'request_time': '2024-04-09T18:15:06+00:00',
            'status_code': 'PENDING',
            'user_id': 'cirrusasf',
            'name': 'LC08_L1TP_138041_20240128_20240207_02_T1',
            'job_parameters': {
                'granules': ['LC08_L1TP_138041_20240128_20240207_02_T1', 'LC08_L1TP_138041_20231227_20240104_02_T1'],
                'parameter_file': '/vsicurl/http://its-live-data.s3.amazonaws.com/'
                'autorift_parameters/v001/autorift_landice_0120m.shp',
                'publish_bucket': '""',
            },
            'credit_cost': 1,
        }
    )

    jobs_expected = sdk.jobs.Batch([job1, job2])
    return jobs_expected


def test_get_stac_item():
    scene = 'LC08_L1TP_138041_20240128_20240207_02_T1'
    expect_item = get_expected_item()

    main.LANDSAT_CATALOG = MagicMock()
    main.LANDSAT_CATALOG.get_collection().get_item.return_value = expect_item

    item = main._get_stac_item(scene)

    assert item.collection_id == expect_item.collection_id
    assert item.properties['instruments'] == expect_item.properties['instruments']
    assert item.properties['landsat:wrs_path'] == expect_item.properties['landsat:wrs_path']
    assert item.properties['landsat:wrs_row'] == expect_item.properties['landsat:wrs_row']
    assert item.properties['view:off_nadir'] == expect_item.properties['view:off_nadir']
    assert item.properties['landsat:cloud_cover_land'] == expect_item.properties['landsat:cloud_cover_land']
    assert item.properties['landsat:collection_category'] == expect_item.properties['landsat:collection_category']


def test_get_landsat_pairs_for_reference_scene():
    main.LANDSAT_CATALOG = MagicMock()
    reference_item = get_expected_item()
    results_item_collection = pystac.item_collection.ItemCollection.from_file(
        'tests/data/scene1_return_itemcollection.json'
    )
    data_gen = (y for y in [results_item_collection])
    main.LANDSAT_CATALOG.search().pages.return_value = data_gen

    df = main.get_landsat_pairs_for_reference_scene(reference_item)

    assert (df['landsat:wrs_path'] == reference_item.properties['landsat:wrs_path']).all()
    assert (df['landsat:wrs_row'] == reference_item.properties['landsat:wrs_row']).all()
    assert (df['view:off_nadir'] == reference_item.properties['view:off_nadir']).all()
    assert (df['instruments'].apply(lambda x: ''.join(x)) == ''.join(reference_item.properties['instruments'])).all()
    assert (df['reference'] == reference_item.id).all()


def test_deduplicate_hyp3_pairs(pairs: gpd.GeoDataFrame = SAMPLE_PAIRS):
    duplicate_jobs = get_expected_jobs()

    main.HYP3 = MagicMock()
    main.HYP3.find_jobs.return_value = duplicate_jobs

    new_pairs = main.deduplicate_hyp3_pairs(pairs)
    main.HYP3 = HYP3_real

    p_idx = pairs.set_index(['reference', 'secondary'])
    np_idx = new_pairs.set_index(['reference', 'secondary'])
    assert np_idx.isin(p_idx).any().any()
    assert len(p_idx) - 2 == len(np_idx)


def test_submit_pairs_for_processing(pairs: gpd.GeoDataFrame = SAMPLE_PAIRS):
    jobs_expect = get_expected_jobs()

    main.HYP3.submit_prepared_jobs = MagicMock()
    main.HYP3.submit_prepared_jobs.return_value = jobs_expect

    jobs = main.submit_pairs_for_processing(pairs)
    main.HYP3 = HYP3_real

    assert jobs == jobs_expect
