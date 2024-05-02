from unittest.mock import MagicMock, patch

import geopandas as gpd
import hyp3_sdk as sdk

import main


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
