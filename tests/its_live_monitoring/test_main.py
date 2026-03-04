import pytest

import main


def test_product_id_from_message(landsat_message, sentinel2_message, sentinel1_burst_message):
    assert 'LC08_L1TP_001005_20230704_20230717_02_T1' == main.product_id_from_message(landsat_message)
    assert 'LOO' == main.product_id_from_message({'landsat_product_id': 'LOO'})
    with pytest.raises(ValueError):
        main.product_id_from_message({'landsat_product_id': 'FOO'})

    assert 'S2B_MSIL1C_20240430T142739_N0510_R139_T24VUR_20240430T162937' == main.product_id_from_message(
        sentinel2_message
    )
    assert 'S2X' == main.product_id_from_message({'name': 'S2X'})
    with pytest.raises(ValueError):
        main.product_id_from_message({'name': 'FOO'})
    with pytest.raises(ValueError):
        main.product_id_from_message({'name': 'S1X'})

    assert 'S1_247728_IW1_20251003T154900_VV_657C-BURST' == main.product_id_from_message(sentinel1_burst_message)
    assert 'S1X' == main.product_id_from_message({'granule_ur': 'S1X'})
    with pytest.raises(ValueError):
        main.product_id_from_message({'granule_ur': 'FOO'})
    with pytest.raises(ValueError):
        main.product_id_from_message({'granule_ur': 'S2X'})
