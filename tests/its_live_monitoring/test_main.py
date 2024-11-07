from unittest.mock import patch

import geopandas as gpd
import hyp3_sdk as sdk
from shapely import Polygon

import main


def test_point_to_region():
    assert main.point_to_region(63.0, 128.0) == 'N60E120'
    assert main.point_to_region(-63.0, 128.0) == 'S60E120'
    assert main.point_to_region(63.0, -128.0) == 'N60W120'
    assert main.point_to_region(-63.0, -128.0) == 'S60W120'
    assert main.point_to_region(0.0, 128.0) == 'N00E120'
    assert main.point_to_region(0.0, -128.0) == 'N00W120'
    assert main.point_to_region(63.0, 0.0) == 'N60E000'
    assert main.point_to_region(-63.0, 0.0) == 'S60E000'
    assert main.point_to_region(0.0, 0.0) == 'N00E000'
    # particularly weird edge cases which can arise if you round the point before passing it in
    assert main.point_to_region(-0.0, 0.0) == 'S00E000'
    assert main.point_to_region(-0.0, -0.0) == 'S00W000'
    assert main.point_to_region(0.0, -0.0) == 'N00W000'


def test_regions_from_bounds():
    assert main.regions_from_bounds(-128.0, -63.0, -109.0, -54.0) == {
        'S60W120',
        'S60W110',
        'S60W100',
        'S50W120',
        'S50W110',
        'S50W100',
    }
    assert main.regions_from_bounds(-5.0, -5.0, 5.0, 5.0) == {'S00W000', 'S00E000', 'N00W000', 'N00E000'}
    assert main.regions_from_bounds(104.0, 53.0, 123.0, 61.0) == {
        'N60E120',
        'N60E110',
        'N60E100',
        'N50E120',
        'N50E110',
        'N50E100',
    }
    assert main.regions_from_bounds(-128.0, -63.0, -128.0, -63.0) == {'S60W120'}


@patch('main.s3.list_objects_v2')
def test_get_key(mock_list_objects_v2):
    mock_list_objects_v2.side_effect = [
        {'Contents': []},
        {
            'Contents': [
                {'Key': 'foo'},
                {'Key': 'bar'},
                {'Key': 'N00E010/earliest_X_latest_G0120V02_P000.nc'},
                {'Key': 'fizz'},
            ]
        },
    ]

    assert main.get_key(['N00E000', 'N00E010'], 'latest', 'earliest') == 'N00E010/earliest_X_latest_G0120V02_P000.nc'


@patch('main.HYP3.find_jobs')
def test_deduplicate_hyp3_pairs(mock_find_jobs, hyp3_batch_factory):
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

    mock_find_jobs.side_effect = [sdk.Batch(), sdk.Batch()]
    pairs = main.deduplicate_hyp3_pairs(landsat_pairs)
    assert pairs.equals(landsat_pairs)

    mock_find_jobs.side_effect = [hyp3_batch_factory(zip(ref_scenes, sec_scenes)), sdk.Batch()]
    pairs = main.deduplicate_hyp3_pairs(landsat_pairs)
    assert len(pairs) == 0

    mock_find_jobs.side_effect = [hyp3_batch_factory(zip(ref_scenes[:-1], sec_scenes[:-1])), sdk.Batch()]
    pairs = main.deduplicate_hyp3_pairs(landsat_pairs)
    assert len(pairs) == 1


@patch('main.get_key')
def test_deduplicate_s3_pairs(mock_get_key):
    sec_scenes = [
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        'LC08_L1TP_138041_20240112_20240123_02_T1',
        'LC09_L1TP_138041_20240104_20240104_02_T1',
    ]
    ref_scenes = ['LC08_L1TP_138041_20240128_20240207_02_T1'] * 3
    ref_acquisitions = ['2024-01-28T04:29:49.361022Z'] * 3
    geometries = [Polygon.from_bounds(0, 0, 1, 1)] * 3

    landsat_pairs = gpd.GeoDataFrame(
        {
            'reference': ref_scenes,
            'secondary': sec_scenes,
            'reference_acquisition': ref_acquisitions,
            'geometry': geometries,
        }
    )

    mock_get_key.side_effect = [None, None, None]
    pairs = main.deduplicate_s3_pairs(landsat_pairs)
    assert pairs.equals(landsat_pairs)

    mock_get_key.side_effect = [None, 'foo', None]
    pairs = main.deduplicate_s3_pairs(landsat_pairs)
    assert pairs.equals(landsat_pairs.drop(1))

    mock_get_key.side_effect = ['foo', 'bar', 'bazz']
    pairs = main.deduplicate_s3_pairs(landsat_pairs)
    assert pairs.equals(landsat_pairs.drop(0).drop(1).drop(2))


@patch('main.HYP3.submit_prepared_jobs')
def test_submit_pairs_for_processing(mock_submit_prepared_jobs, hyp3_batch_factory):
    sec_scenes = [
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        'LC08_L1TP_138041_20240112_20240123_02_T1',
        'LC09_L1TP_138041_20240104_20240104_02_T1',
    ]
    ref_scenes = ['LC08_L1TP_138041_20240128_20240207_02_T1'] * 3

    landsat_jobs = hyp3_batch_factory(zip(ref_scenes, sec_scenes))
    landsat_pairs = gpd.GeoDataFrame({'reference': ref_scenes, 'secondary': sec_scenes})

    mock_submit_prepared_jobs.side_effect = [landsat_jobs]
    jobs = main.submit_pairs_for_processing(landsat_pairs)
    assert jobs == landsat_jobs
