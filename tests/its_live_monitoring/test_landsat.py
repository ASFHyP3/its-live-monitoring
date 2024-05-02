from copy import deepcopy
from datetime import datetime
from unittest.mock import MagicMock, patch

import landsat


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

    with patch('landsat.LANDSAT_COLLECTION', MagicMock()):
        landsat.LANDSAT_COLLECTION.get_item.return_value = expected_item
        item = landsat.get_landsat_stac_item(scene)

    assert item.collection_id == collection
    assert item.properties == properties


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
    assert landsat.qualifies_for_landsat_processing(good_item)

    item = deepcopy(good_item)
    item.collection_id = 'foo'
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['instruments'] = ['TIRS']
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:collection_category'] = 'T2'
    assert landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:collection_category'] = 'RT'
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:wrs_path'] = 'foo'
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:wrs_row'] = 'foo'
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    del item.properties['landsat:cloud_cover_land']
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = -1
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = 0
    assert landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = 1
    assert landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = landsat.MAX_CLOUD_COVER_PERCENT - 1
    assert landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = landsat.MAX_CLOUD_COVER_PERCENT
    assert landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['landsat:cloud_cover_land'] = landsat.MAX_CLOUD_COVER_PERCENT + 1
    assert not landsat.qualifies_for_landsat_processing(item)

    item = deepcopy(good_item)
    item.properties['view:off_nadir'] = 14.065
    assert landsat.qualifies_for_landsat_processing(item)


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
        datetime='2024-01-28T04:29:49.361022Z',
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
            pystac_item_factory(id=scene, datetime=date_time, properties=properties, collection=collection)
        )

    with patch('landsat.LANDSAT_CATALOG', MagicMock()):
        landsat.LANDSAT_CATALOG.search().pages.return_value = (sec_items,)
        df = landsat.get_landsat_pairs_for_reference_scene(ref_item)

    assert (df['landsat:wrs_path'] == ref_item.properties['landsat:wrs_path']).all()
    assert (df['landsat:wrs_row'] == ref_item.properties['landsat:wrs_row']).all()
    assert (df['view:off_nadir'] == ref_item.properties['view:off_nadir']).all()
    assert (df['instruments'].apply(lambda x: ''.join(x)) == ''.join(ref_item.properties['instruments'])).all()
    assert (df['reference'] == ref_item.id).all()


def test_get_landsat_pairs_for_off_nadir_reference_scene(pystac_item_factory):
    properties = {
        'instruments': ['OLI'],
        'landsat:collection_category': 'T1',
        'landsat:wrs_path': '001',
        'landsat:wrs_row': '005',
        'landsat:cloud_cover_land': 50,
        'view:off_nadir': 14.065,
    }
    collection = 'landsat-c2l1'

    ref_item = pystac_item_factory(
        id='LC08_L1TP_138041_20240128_20240207_02_T1',
        datetime='2024-01-28T04:29:49.361022Z',
        properties=properties,
        collection=collection,
    )

    sec_scenes = [
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        'LC08_L1TP_138041_20240112_20240123_02_T1',
        'LC09_L1TP_138041_20240104_20240104_02_T1',
    ]
    sec_date_times = ['2024-01-20T04:30:03.658618Z', '2024-01-12T04:29:55.948514Z', '2024-01-04T04:30:03.184014Z']
    sec_off_nadir_angles = [14.049, 14.147, 14.100]
    sec_items = []
    for scene, date_time, off_nadir in zip(sec_scenes, sec_date_times, sec_off_nadir_angles):
        props = deepcopy(properties)
        props['view:off_nadir'] = off_nadir
        sec_items.append(pystac_item_factory(id=scene, datetime=date_time, properties=props, collection=collection))

    with patch('landsat.LANDSAT_CATALOG', MagicMock()):
        landsat.LANDSAT_CATALOG.search().pages.return_value = (sec_items,)
        df = landsat.get_landsat_pairs_for_reference_scene(ref_item)

    assert (df['view:off_nadir'] > 0).all()

    assert (df['landsat:wrs_path'] == ref_item.properties['landsat:wrs_path']).all()
    assert (df['landsat:wrs_row'] == ref_item.properties['landsat:wrs_row']).all()
    assert (df['instruments'].apply(lambda x: ''.join(x)) == ''.join(ref_item.properties['instruments'])).all()
    assert (df['reference'] == ref_item.id).all()
