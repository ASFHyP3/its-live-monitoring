from datetime import datetime
from dateutil.tz import tzutc
from unittest.mock import MagicMock, patch

import geopandas as gpd
import hyp3_sdk as sdk

import main

def test_get_landsat_stac_item(pystac_item_factory):
    scene = 'LC08_L1TP_138041_20240128_20240207_02_T1'
    properties = {
        'instruments': ['OLI'],
        'landsat:collection_category': 'T1',
        'landsat:wrs_path': '001',
        'landsat:wrs_row': '005',
        'landsat:cloud_cover_land': 50,
        'view:off_nadir': 0,
    }
    collection = 'landsat-c2l1'
    expected_item = pystac_item_factory(id=scene, datetime=datetime.now(), properties=properties, collection=collection)

    with patch('main.LANDSAT_CATALOG', MagicMock()):
        main.LANDSAT_CATALOG.get_collection().get_item.return_value = expected_item
        item = main._get_stac_item(scene, main.LANDSAT_CATALOG.get_collection())

    assert item.collection_id == collection
    assert item.properties == properties


def test_get_sentinel2_stac_item(pystac_item_factory):
    scene = 'S2B_13CES_20200315_0_L1C'
    properties = {
        'created': '2022-11-06T07:09:52.078Z',
        'instruments': ['msi'],
        'eo:cloud_cover': 28.1884,
        'mgrs:utm_zone': 13,
        'mgrs:latitude_band': 'C',
        'mgrs:grid_square': 'ES',
        's2:product_uri': '2B_MSIL1C_20200315T152259_N0209_R039_T13CES_20200315T181115.SAFE',
    }
    collection = 'sentinel-2-l1c'
    dt = datetime(2020, 3, 15, 15, 24, 29, 455000, tzinfo=tzutc())
    expected_item = pystac_item_factory(id=scene, datetime=dt, properties=properties, collection=collection)

    with patch('main.SENTINEL2_CATALOG', MagicMock()):
        main.SENTINEL2_CATALOG.get_collection().get_item.return_value = expected_item
        item = main._get_stac_item(scene, main.SENTINEL2_CATALOG.get_collection())

    assert item.collection_id == collection
    assert item.properties == properties


def test_deduplicate_hyp3_pairs(hyp3_batch_factory):
    sec_scenes = [
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        'LC08_L1TP_138041_20240112_20240123_02_T1',
        'LC09_L1TP_138041_20240104_20240104_02_T1',
    ]
    ref_scenes = ['LC08_L1TP_138041_20240128_20240207_02_T1'] * 3
    ref_acquisitions = ['2024-01-28T04:29:49.361022Z'] * 3

    landsat_pairs = gpd.GeoDataFrame(
        {'reference': ref_scenes, 'secondary': sec_scenes, 'reference_acquisition': ref_acquisitions}
    )

    with patch('main.HYP3.find_jobs', MagicMock(return_value=sdk.Batch())):
        pairs = main.deduplicate_hyp3_pairs(landsat_pairs)

    assert pairs.equals(landsat_pairs)

    landsat_jobs = hyp3_batch_factory(zip(ref_scenes, sec_scenes))
    with patch('main.HYP3.find_jobs', MagicMock(return_value=landsat_jobs)):
        pairs = main.deduplicate_hyp3_pairs(landsat_pairs)

    assert len(pairs) == 0

    landsat_jobs = hyp3_batch_factory(zip(ref_scenes[:-1], sec_scenes[:-1]))
    with patch('main.HYP3.find_jobs', MagicMock(return_value=landsat_jobs)):
        pairs = main.deduplicate_hyp3_pairs(landsat_pairs)

    assert len(pairs) == 1


def test_submit_pairs_for_processing(hyp3_batch_factory):
    sec_scenes = [
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        'LC08_L1TP_138041_20240112_20240123_02_T1',
        'LC09_L1TP_138041_20240104_20240104_02_T1',
    ]
    ref_scenes = ['LC08_L1TP_138041_20240128_20240207_02_T1'] * 3

    landsat_jobs = hyp3_batch_factory(zip(ref_scenes, sec_scenes))
    landsat_pairs = gpd.GeoDataFrame({'reference': ref_scenes, 'secondary': sec_scenes})

    with patch('main.HYP3.submit_prepared_jobs', MagicMock(return_value=landsat_jobs)):
        jobs = main.submit_pairs_for_processing(landsat_pairs)

    assert jobs == landsat_jobs