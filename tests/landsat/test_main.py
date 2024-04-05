import unittest.mock
import pickle

from landsat.src import main

import geopandas as gpd

import pdb


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


def test_get_stac_item():
    scene1 = 'LC08_L1TP_138041_20240128_20240207_02_T1'
    item1_id = 'LC08_L1TP_138041_20240128_20240207_02_T1'
    item1_properties = {'datetime': '2024-01-28T04:29:49.361022Z',
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
                        'created': '2024-02-07T18:27:27.558Z',
                        'updated': '2024-02-07T18:27:27.558Z'
                        }

    assert (main._get_stac_item(scene1).id == item1_id
            and main._get_stac_item(scene1).properties == item1_properties)

    scene2 = 'LC08_L1GT_208119_20190225_20200829_02_T2'

    item2_id = 'LC08_L1GT_208119_20190225_20200829_02_T2'

    item2_properties = {'datetime': '2019-02-25T12:13:15.140373Z',
                        'eo:cloud_cover': 46.47,
                        'view:sun_azimuth': 87.63621418,
                        'view:sun_elevation': 9.4970641,
                        'platform': 'LANDSAT_8',
                        'instruments': ['OLI', 'TIRS'],
                        'view:off_nadir': 14.092,
                        'landsat:cloud_cover_land': 46.47,
                        'landsat:wrs_type': '2',
                        'landsat:wrs_path': '208',
                        'landsat:wrs_row': '119',
                        'landsat:scene_id': 'LC82081192019056LGN00',
                        'landsat:collection_category': 'T2',
                        'landsat:collection_number': '02',
                        'landsat:correction': 'L1GT',
                        'proj:epsg': 3031,
                        'proj:shape': [8901, 9151],
                        'proj:transform': [30, 0, -980415, 0, -30, 188115],
                        'created': '2022-07-06T18:15:03.664Z',
                        'updated': '2022-07-06T18:15:03.664Z'
                        }

    assert (main._get_stac_item(scene2).id == item2_id and
            main._get_stac_item(scene2).properties == item2_properties)


def test_get_landsat_pairs_for_reference_scene():
    item = main._get_stac_item('LC08_L1TP_138041_20240128_20240207_02_T1')
    df = main.get_landsat_pairs_for_reference_scene(item)
    with open("tests/data/LC08_L1TP_138041_20240128_20240207_02_T1_pairs.pkl", 'rb') as file:
        df_expect = pickle.load(file)
        assert df.equals(df_expect)

    item = main._get_stac_item('LC08_L1GT_208119_20190225_20200829_02_T2')
    df = main.get_landsat_pairs_for_reference_scene(item)
    with open("tests/data/LC08_L1GT_208119_20190225_20200829_02_T2_pairs.pkl", 'rb') as file:
        df_expect = pickle.load(file)
        assert df.equals(df_expect)
