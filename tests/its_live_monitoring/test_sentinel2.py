from copy import deepcopy
from datetime import datetime
from unittest.mock import MagicMock, patch

import sentinel2


def test_get_sentinel2_stac_item(pystac_item_factory):
    scene = 'S2B_MSIL1C_20200315T152259_N0209_R039_T13CES_20200315T181115.SAFE'
    properties = {
        'tileId': '13CES',
        'cloudCover': 28.188400000000005,
        'productType': 'S2MSI1C',
        'instrumentShortName': 'MSI',
    }
    collection = 'SENTINEL-2'
    date_time = '2020-03-15T15:22:59.024Z'
    expected_item = pystac_item_factory(id=scene, datetime=date_time, properties=properties, collection=collection)

    with patch('sentinel2.SENTINEL2_COLLECTION', MagicMock()):
        sentinel2.SENTINEL2_COLLECTION.get_item.return_value = expected_item
        item = sentinel2.get_sentinel2_stac_item(scene)

    assert item.collection_id == collection
    assert item.properties == properties


def test_qualifies_for_processing(pystac_item_factory):
    properties = {
        'tileId': '19DEE',
        'cloudCover': 30,
        'productType': 'S2MSI1C',
        'instrumentShortName': 'MSI',
    }
    collection = 'SENTINEL-2'

    good_item = pystac_item_factory(
        id='XXX_XXXL1C_XXXX', datetime=datetime.now(), properties=properties, collection=collection
    )

    assert sentinel2.qualifies_for_sentinel2_processing(good_item)

    item = deepcopy(good_item)
    item.collection_id = 'foo'
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['productType'] = 'S2MSI2A'
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['instrumentShortName'] = 'MIS'
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['tileId'] = '30BZZ'
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    del item.properties['cloudCover']
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['cloudCover'] = -1
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['cloudCover'] = 0
    assert sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['cloudCover'] = 1
    assert sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['cloudCover'] = sentinel2.MAX_CLOUD_COVER_PERCENT - 1
    assert sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['cloudCover'] = sentinel2.MAX_CLOUD_COVER_PERCENT
    assert sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['cloudCover'] = sentinel2.MAX_CLOUD_COVER_PERCENT + 1
    assert not sentinel2.qualifies_for_sentinel2_processing(item)


def test_get_sentinel2_pairs_for_reference_scene(pystac_item_factory):
    scene = 'S2B_MSIL1C_20240430T143829_N0510_R139_T22TCR_20240430T162923.SAFE'
    properties = {
        'cloudCover': 28.1884,
        'tileId': '13CES',
        'productType': 'S2MSI1C',
        'instrumentShortName': 'MSI',
    }
    collection = 'SENTINEL-2'
    date_time = '2024-04-30T14:38:29.024Z'
    ref_item = pystac_item_factory(id=scene, datetime=date_time, properties=properties, collection=collection)

    sec_scenes = [
        'S2B_MSIL1C_20240130T000000_N0510_R139_T22TCR_20240430T000000',
        'S2A_MSIL1C_20230824T000000_N0510_R139_T22TCR_20230824T000000',
        'S2B_MSIL1C_20220101T000000_N0510_R139_T22TCR_20220101T000000',
    ]
    sec_date_times = [
        '2024-01-30T00:00:00.000Z',
        '2023-08-24T00:00:00.000Z',
        '2022-01-01T00:00:00.000Z',
    ]
    sec_items = []
    for scene, date_time in zip(sec_scenes, sec_date_times):
        props = deepcopy(properties)
        sec_items.append(pystac_item_factory(id=scene, datetime=date_time, properties=props, collection=collection))

    with patch('sentinel2.SENTINEL2_CATALOG', MagicMock()):
        sentinel2.SENTINEL2_CATALOG.search().pages.return_value = (sec_items,)
        df = sentinel2.get_sentinel2_pairs_for_reference_scene(ref_item)

    assert (df['tileId'] == ref_item.properties['tileId']).all()
    assert (df['instrumentShortName'] == ref_item.properties['instrumentShortName']).all()
    assert (df['reference_acquisition'] == ref_item.datetime).all()
