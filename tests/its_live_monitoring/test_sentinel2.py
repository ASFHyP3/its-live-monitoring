import unittest.mock
from copy import deepcopy
from datetime import datetime
from unittest.mock import MagicMock, patch

from dateutil.tz import tzutc

import sentinel2


def test_qualifies_for_processing(pystac_item_factory):
    properties = {
        'instruments': ['msi'],
        'mgrs:utm_zone': '19',
        'mgrs:latitude_band': 'D',
        'mgrs:grid_square': 'EE',
        'eo:cloud_cover': 30,
    }
    collection = 'sentinel-2-l1c'

    good_item = pystac_item_factory(
        id='sentinel2-scene', datetime=datetime.now(), properties=properties, collection=collection
    )

    assert sentinel2.qualifies_for_sentinel2_processing(good_item)

    item = deepcopy(good_item)
    item.collection_id = 'foo'
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['instruments'] = ['mis']
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['mgrs:utm_zone'] = '30'
    item.properties['mgrs:latitude_band'] = 'B'
    item.properties['mgrs:grid_square'] = 'ZZ'
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['eo:cloud_cover'] = 75
    assert not sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['eo:cloud_cover'] = 0
    assert sentinel2.qualifies_for_sentinel2_processing(item)

    item = deepcopy(good_item)
    item.properties['eo:cloud_cover'] = -1
    assert not sentinel2.qualifies_for_sentinel2_processing(item)


def test_get_sentinel2_pairs_for_reference_scene(pystac_item_factory):
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
    ref_item = pystac_item_factory(id=scene, datetime=dt, properties=properties, collection=collection)

    sec_scenes = [
        'S2B_13CES_20200224_0_L1C',
        'S2B_13CES_20200211_0_L1C',
        'S2B_13CES_20200201_0_L1C',
    ]
    sec_date_times = [
        '2020-02-24 15:24:28.312000+00:00',
        '2020-02-11 15:14:28.467000+00:00',
        '2020-02-01 15:14:26.405000+00:00',
    ]
    sec_items = []
    for scene, date_time in zip(sec_scenes, sec_date_times):
        props = deepcopy(properties)
        sec_items.append(pystac_item_factory(id=scene, datetime=date_time, properties=props, collection=collection))

    with patch('sentinel2.SENTINEL2_CATALOG', MagicMock()):
        sentinel2.SENTINEL2_CATALOG.search().pages.return_value = (sec_items,)
        df = sentinel2.get_sentinel2_pairs_for_reference_scene(ref_item)

    assert (df['mgrs:utm_zone'] == ref_item.properties['mgrs:utm_zone']).all()
    assert (df['mgrs:latitude_band'] == ref_item.properties['mgrs:latitude_band']).all()
    assert (df['mgrs:grid_square'] == ref_item.properties['mgrs:grid_square']).all()
    assert (df['instruments'].apply(lambda x: ''.join(x)) == ''.join(ref_item.properties['instruments'])).all()
    assert (df['referenceId'] == ref_item.id).all()
