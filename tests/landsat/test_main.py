from copy import deepcopy
from datetime import datetime
from unittest.mock import MagicMock, patch

import geopandas as gpd
import hyp3_sdk as sdk
from dateutil.parser import parse as date_parser

import main


def test_qualifies_for_processing(pystac_item_factory):
    properties = {
        'instruments': ['OLI'],
        'landsat:collection_category': 'T1',
        'landsat:wrs_path': '001',
        'landsat:wrs_row': '005',
        'landsat:cloud_cover_land': 50,
        'view:off_nadir': 0,
    }
    collection = 'landsat-c2l1'

    good_item = pystac_item_factory(
        id='landsat-scene', datetime=datetime.now(), properties=properties, collection=collection
    )
    assert main._qualifies_for_processing(good_item)

    item = deepcopy(good_item)
    item.collection_id = 'foo'
    assert not main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['instruments'] = ['TIRS']
    assert not main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:collection_category'] = 'T2'
    assert main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:collection_category'] = 'RT'
    assert not main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:wrs_path'] = 'foo'
    assert not main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:wrs_row'] = 'foo'
    assert not main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    del item.properties['landsat:cloud_cover_land']
    assert not main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = -1
    assert not main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = 0
    assert main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = 1
    assert main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = main.MAX_CLOUD_COVER_PERCENT - 1
    assert main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = main.MAX_CLOUD_COVER_PERCENT
    assert main._qualifies_for_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = main.MAX_CLOUD_COVER_PERCENT + 1
    assert not main._qualifies_for_processing(item)


def test_get_stac_item(pystac_item_factory):
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
        item = main._get_stac_item(scene)

    assert item.collection_id == collection
    assert item.properties == properties


def test_get_landsat_pairs_for_reference_scene(pystac_item_factory):
    properties = {
        'instruments': ['OLI'],
        'landsat:collection_category': 'T1',
        'landsat:wrs_path': '001',
        'landsat:wrs_row': '005',
        'landsat:cloud_cover_land': 50,
        'view:off_nadir': 0,
    }
    collection = 'landsat-c2l1'

    ref_item = pystac_item_factory(
        id='LC08_L1TP_138041_20240128_20240207_02_T1',
        datetime=date_parser('2024-01-28T04:29:49.361022Z'),
        properties=properties,
        collection=collection,
    )

    sec_scenes = [
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        'LC08_L1TP_138041_20240112_20240123_02_T1',
        'LC09_L1TP_138041_20240104_20240104_02_T1',
    ]
    sec_date_times = ['2024-01-20T04:30:03.658618Z', '2024-01-12T04:29:55.948514Z', '2024-01-04T04:30:03.184014Z']
    sec_items = []
    for scene, date_time in zip(sec_scenes, sec_date_times):
        sec_items.append(
            pystac_item_factory(id=scene, datetime=date_parser(date_time), properties=properties, collection=collection)
        )

    with patch('main.LANDSAT_CATALOG', MagicMock()):
        main.LANDSAT_CATALOG.search().pages.return_value = (sec_items,)
        df = main.get_landsat_pairs_for_reference_scene(ref_item)

    assert (df['landsat:wrs_path'] == ref_item.properties['landsat:wrs_path']).all()
    assert (df['landsat:wrs_row'] == ref_item.properties['landsat:wrs_row']).all()
    assert (df['view:off_nadir'] == ref_item.properties['view:off_nadir']).all()
    assert (df['instruments'].apply(lambda x: ''.join(x)) == ''.join(ref_item.properties['instruments'])).all()
    assert (df['reference'] == ref_item.id).all()


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
