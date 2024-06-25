from copy import deepcopy
from datetime import datetime
from unittest.mock import MagicMock, patch

import pystac
import pytest
import requests
import responses

import sentinel2


@responses.activate
def test_raise_for_missing_in_google_cloud():
    existing_scene = 'S2B_MSIL1C_20200315T152259_N0209_R039_T13CES_20200315T181115'
    missing_scene = 'S2B_MSIL1C_20200315T152259_N0209_R039_T13CES_00010101T000000'

    root_url = 'https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles'

    responses.head(f'{root_url}/13/C/ES/{existing_scene}.SAFE/manifest.safe', status=200)
    responses.head(f'{root_url}/13/C/ES/{missing_scene}.SAFE/manifest.safe', status=404)

    assert sentinel2.raise_for_missing_in_google_cloud(existing_scene) is None

    with pytest.raises(requests.HTTPError):
        sentinel2.raise_for_missing_in_google_cloud(missing_scene)


def test_get_sentinel2_stac_item(pystac_item_factory):
    scene = 'S2B_13CES_20200315_0_L1C'
    properties = {
        'grid:code': 'MGRS-13CES',
        'eo:cloud_cover': 28.188400000000005,
        's2:product_type': 'S2MSI1C',
        's2:product_uri': 'S2B_MSIL1C_20200315T152259_N0209_R039_T13CES_20200315T181115.SAFE',
        'instruments': ['msi'],
    }
    collection = 'sentinel-2-l1c'
    date_time = '2020-03-15T15:22:59.024Z'
    expected_item = pystac_item_factory(id=scene, datetime=date_time, properties=properties, collection=collection)

    class MockItemSearch:
        def __init__(self, item: pystac.item.Item):
            self.items = [item] if item else []

        def pages(self):
            return [self.items]

    with patch('sentinel2.SENTINEL2_CATALOG', MagicMock()):
        sentinel2.SENTINEL2_CATALOG.search.return_value = MockItemSearch(expected_item)
        item = sentinel2.get_sentinel2_stac_item(scene)

    assert item.collection_id == collection
    assert item.properties == properties

    with patch('sentinel2.SENTINEL2_CATALOG', MagicMock()):
        sentinel2.SENTINEL2_CATALOG.search.return_value = MockItemSearch(None)
        with pytest.raises(ValueError):
            item = sentinel2.get_sentinel2_stac_item(scene)


def test_qualifies_for_processing(pystac_item_factory):
    properties = {
        'grid:code': 'MGRS-19DEE',
        'eo:cloud_cover': 30,
        's2:product_uri': 'S2B_MSIL1C_20240528T000000_N0510_R110_T22TCR_20240528T000000.SAFE',
        's2:product_type': 'S2MSI1C',
        'instruments': ['msi'],
    }
    collection = 'sentinel-2-l1c'
    good_item = pystac_item_factory(
        id='XXX_XXXL1C_XXXX_XXXX_XXXX', datetime=datetime.now(), properties=properties, collection=collection
    )

    with patch('sentinel2.get_data_coverage_for_item', (lambda x: 75.0)):
        assert sentinel2.qualifies_for_sentinel2_processing(good_item)

        item = deepcopy(good_item)
        item.collection_id = 'foo'
        assert not sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['s2:product_type'] = 'S2MSI2A'
        assert not sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['instruments'] = ['mis']
        assert not sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['grid:code'] = 'MGRS-30BZZ'
        assert not sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        del item.properties['eo:cloud_cover']
        assert not sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['eo:cloud_cover'] = -1
        assert not sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['eo:cloud_cover'] = 0
        assert sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['eo:cloud_cover'] = 1
        assert sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['eo:cloud_cover'] = sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT - 1
        assert sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['eo:cloud_cover'] = sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT
        assert sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        item.properties['eo:cloud_cover'] = sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT + 1
        assert not sentinel2.qualifies_for_sentinel2_processing(item)

        item = deepcopy(good_item)
        assert sentinel2.qualifies_for_sentinel2_processing(item, relative_orbit='R110')

        assert not sentinel2.qualifies_for_sentinel2_processing(item, relative_orbit='R100')

    with patch('sentinel2.get_data_coverage_for_item', (lambda x: 50.0)):
        assert not sentinel2.qualifies_for_sentinel2_processing(good_item)


def test_get_sentinel2_pairs_for_reference_scene(pystac_item_factory):
    scene = 'S2B_22TCR_20240528_0_L1C'
    properties = {
        'eo:cloud_cover': 28.1884,
        'grid:code': 'MGRS-13CES',
        's2:product_uri': 'S2B_MSIL1C_20240528T000000_N0510_R110_T22TCR_20240528T000000.SAFE',
        's2:product_type': 'S2MSI1C',
        'instruments': ['msi'],
    }
    collection = 'sentinel-2-l1c'
    date_time = '2024-05-28T00:00:00.000Z'
    geometry = {
        'type': 'Polygon',
        'coordinates': [
            [
                [-52.15438338757846, 45.48626501919109],
                [-52.16589191027769, 46.0476276229589],
                [-53.58406512039718, 46.02435339629007],
                [-53.57322833884183, 45.7857967705479],
                [-53.32496669303952, 45.73822984331193],
                [-53.32508777963369, 45.73791866117696],
                [-53.30873391294973, 45.73461830894227],
                [-53.30848609469098, 45.73525483077776],
                [-53.30759736933965, 45.73507503099143],
                [-53.30756593451088, 45.73515581845892],
                [-53.02987907793318, 45.67896916634812],
                [-53.03003414034234, 45.67857563154723],
                [-53.02996374324132, 45.67856140680902],
                [-53.0300373999091, 45.67837493375828],
                [-53.02992182400236, 45.6783516095372],
                [-53.03001446656566, 45.6781166237772],
                [-52.71741532322856, 45.6127477580404],
                [-52.71588433942618, 45.61240870105769],
                [-52.7156314612003, 45.61304073542762],
                [-52.71459837053103, 45.61280975489838],
                [-52.71454892075972, 45.61293346903908],
                [-52.43459853871974, 45.55057718314298],
                [-52.43472193557562, 45.55027212333509],
                [-52.43454539896989, 45.55023291790532],
                [-52.43459400875725, 45.55011316884261],
                [-52.43433679669845, 45.550056205768],
                [-52.43443395519819, 45.54981618062912],
                [-52.15438338757846, 45.48626501919109],
            ]
        ],
    }
    ref_item = pystac_item_factory(
        id=scene, datetime=date_time, properties=properties, collection=collection, geometry=geometry
    )
    sec_scenes = [
        'S2B_22TCR_20240528_0_L1C',
        'S2B_22TCR_20230528_0_L1C',
        'S2B_22TCR_20210528_0_L1C',
    ]
    sec_date_times = [
        '2024-05-28T00:00:00.000Z',
        '2023-05-28T00:00:00.000Z',
        '2021-05-28T00:00:00.000Z',
    ]
    sec_items = []
    for scene, date_time in zip(sec_scenes, sec_date_times):
        props = deepcopy(properties)
        sec_items.append(pystac_item_factory(id=scene, datetime=date_time, properties=props, collection=collection))

    with patch('sentinel2.SENTINEL2_CATALOG', MagicMock()):
        sentinel2.SENTINEL2_CATALOG.search().pages.return_value = (sec_items,)
        with patch('sentinel2.get_data_coverage_for_item', (lambda x: 75.0)):
            df = sentinel2.get_sentinel2_pairs_for_reference_scene(ref_item)

    assert (df['grid:code'] == ref_item.properties['grid:code']).all()
    for instrument in df['instruments']:
        assert instrument == ref_item.properties['instruments']
    assert (df['reference_acquisition'] == ref_item.datetime).all()


@responses.activate
def test_get_data_coverage_for_item(pystac_item_factory):
    tile_path = 'sentinel-s2-l1c/tiles/13/C/ES/2024/5/28/0/tileInfo.json'
    assets = {'tileinfo_metadata': pystac.Asset(href=f's3://{tile_path}')}
    item_s3 = pystac_item_factory(
        id='scene_name', datetime='2024-05-28T00:00:00.000Z', properties={}, collection='collection', assets=assets
    )
    item_roda = deepcopy(item_s3)
    item_roda.assets = {'tileinfo_metadata': pystac.Asset(href=f'https://roda.sentinel-hub.com/{tile_path}')}
    url = f'https://roda.sentinel-hub.com/{tile_path}'
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, json={'dataCoveragePercentage': 99.0}, status=200)
        assert sentinel2.get_data_coverage_for_item(item_s3) == 99.0
        assert sentinel2.get_data_coverage_for_item(item_roda) == 99.0
        rsps.add(responses.GET, url, status=404)
        with pytest.raises(requests.HTTPError):
            sentinel2.get_data_coverage_for_item(item_s3)
        with pytest.raises(requests.HTTPError):
            sentinel2.get_data_coverage_for_item(item_roda)


def test_is_new_scene():
    assert sentinel2.is_new_scene('S2B_MSIL1C_20240528T000000_N0510_R110_T22TCR_20240528T000000')
    assert sentinel2.is_new_scene('S2B_MSIL1C_20200315T152259_N0209_R039_T13CES_20200315T181115')
    assert not sentinel2.is_new_scene('S2A_MSIL1C_20201005T020451_N0500_R017_T51MVP_20230307T222553')
    assert not sentinel2.is_new_scene('S2B_MSIL1C_20201017T164309_N0500_R126_T15SYD_20230310T055907')
