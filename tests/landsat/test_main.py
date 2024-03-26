import unittest.mock

import main


def get_mock_pystac_item() -> unittest.mock.NonCallableMagicMock:
    item = unittest.mock.NonCallableMagicMock()
    item.collection_id = 'landsat-c2l1'
    item.properties = {
        'instruments': ['OLI'],
        'landsat:collection_category': 'T1',
        'landsat:wrs_path': '001',
        'landsat:wrs_row': '005',
        'eo:cloud_cover': 50,
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
    item.properties['eo:cloud_cover'] = 59
    assert main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['eo:cloud_cover'] = 60
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['eo:cloud_cover'] = 61
    assert not main._qualifies_for_processing(item)

    item = get_mock_pystac_item()
    item.properties['view:off_nadir'] = 1
    assert not main._qualifies_for_processing(item)
