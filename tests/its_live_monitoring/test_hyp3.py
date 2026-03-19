import datetime
from unittest.mock import patch

import geopandas as gpd
import hyp3_sdk as sdk
import requests

import hyp3


def test_format_time():
    # TODO
    pass


def test_query_jobs_by_status_code(tables):
    its_live_user = 'hyp3.its_live'

    table_items = [
        {
            'job_id': 'job1',
            'user_id': its_live_user,
            'status_code': 'PENDING',
            'request_time': '2024-01-28T00:00:00+00:00',
            'job_type': 'AUTORIFT',
            'name': 'LC09_L1TP_138041_20240120_20240120_02_T1',
        },
        {
            'job_id': 'job2',
            'user_id': its_live_user,
            'status_code': 'PENDING',
            'request_time': '2024-01-29T00:00:00+00:00',
            'job_type': 'AUTORIFT',
            'name': 'LC09_L1TP_138041_20240120_20240120_02_T2',
        },
        {
            'job_id': 'job3',
            'user_id': its_live_user,
            'status_code': 'PENDING',
            'request_time': '2024-01-01T00:00:00+00:00',
            'job_type': 'AUTORIFT',
            'name': 'LC09_L1TP_138041_20240120_20240120_02_T1',
        },
        {
            'job_id': 'job4',
            'user_id': 'other-user',
            'status_code': 'PENDING',
            'request_time': '2024-01-29T00:00:00+00:00',
            'job_type': 'AUTORIFT',
            'name': 'LC09_L1TP_138041_20240120_20240120_02_T1',
        },
        {
            'job_id': 'job5',
            'user_id': 'other-user',
            'status_code': 'RUNNING',
            'request_time': '2024-01-29T00:00:00+00:00',
            'job_type': 'AUTORIFT',
            'name': 'LC09_L1TP_138041_20240120_20240120_02_T1',
        },
    ]

    for item in table_items:
        tables.jobs_table.put_item(Item=item)

    jobs = hyp3.query_jobs_by_status_code(
        'PENDING',
        its_live_user,
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        datetime.datetime.fromisoformat('2024-01-28T00:00:00+00:00'),
    )
    assert jobs == sdk.Batch([sdk.Job.from_dict(table_items[0])])

    jobs = hyp3.query_jobs_by_status_code(
        'RUNNING',
        'other-user',
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        datetime.datetime.fromisoformat('2024-01-01T00:00:00+00:00'),
    )
    assert jobs == sdk.Batch([sdk.Job.from_dict(table_items[4])])

    jobs = hyp3.query_jobs_by_status_code(
        'PENDING',
        its_live_user,
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        datetime.datetime.fromisoformat('2024-01-30T00:00:00+00:00'),
    )
    assert jobs == sdk.Batch([])

    jobs = hyp3.query_jobs_by_status_code(
        'RUNNING',
        its_live_user,
        'LC09_L1TP_138041_20240120_20240120_02_T1',
        datetime.datetime.fromisoformat('2024-01-28T00:00:00+00:00'),
    )
    assert jobs == sdk.Batch([])

    jobs = hyp3.query_jobs_by_status_code(
        'SUCCEEDED',
        'non-existant-user',
        'non-existant-granule',
        datetime.datetime.fromisoformat('2000-01-01T00:00:00+00:00'),
    )
    assert jobs == sdk.Batch([])


def test_reference_secondary_from_job():
    # TODO
    pass


@patch('hyp3.query_jobs_by_status_code')
def test_deduplicate_hyp3_pairs(mock_query_jobs_by_status_code, hyp3_batch_factory):
    sec_scenes = [
        ('LC09_L1TP_138041_20240120_20240120_02_T1',),
        ('LC08_L1TP_138041_20240112_20240123_02_T1',),
        ('LC09_L1TP_138041_20240104_20240104_02_T1',),
    ]
    ref_scenes = [('LC08_L1TP_138041_20240128_20240207_02_T1',)] * 3
    ref_acquisitions = ['2024-01-28T04:29:49.361022Z'] * 3
    names = ['LC08_L1TP_138041_20240128_20240207_02_T1'] * 3

    landsat_pairs = gpd.GeoDataFrame(
        {'reference': ref_scenes, 'secondary': sec_scenes, 'reference_acquisition': ref_acquisitions, 'job_name': names}
    )

    mock_query_jobs_by_status_code.side_effect = [sdk.Batch(), sdk.Batch()]
    pairs = hyp3.deduplicate_hyp3_pairs(landsat_pairs)
    assert pairs.equals(landsat_pairs)

    mock_query_jobs_by_status_code.side_effect = [hyp3_batch_factory(zip(ref_scenes, sec_scenes)), sdk.Batch()]
    pairs = hyp3.deduplicate_hyp3_pairs(landsat_pairs)
    assert len(pairs) == 0

    mock_query_jobs_by_status_code.side_effect = [
        hyp3_batch_factory(zip(ref_scenes[:-1], sec_scenes[:-1])),
        sdk.Batch(),
    ]
    pairs = hyp3.deduplicate_hyp3_pairs(landsat_pairs)
    assert len(pairs) == 1


def test_nullable_str():
    # TODO
    pass


def test_nullable_int():
    # TODO
    pass


@patch('hyp3_sdk.HyP3.submit_prepared_jobs')
@patch('hyp3_sdk.util.get_authenticated_session')
def test_submit_pairs_for_processing(mock_get_authenticated_session, mock_submit_prepared_jobs, hyp3_batch_factory):
    sec_scenes = [
        ('LC09_L1TP_138041_20240120_20240120_02_T1',),
        ('LC08_L1TP_138041_20240112_20240123_02_T1',),
        ('LC09_L1TP_138041_20240104_20240104_02_T1',),
    ]
    ref_scenes = [('LC08_L1TP_138041_20240128_20240207_02_T1',)] * 3
    names = ['LC08_L1TP_138041_20240128_20240207_02_T1'] * 3

    landsat_jobs = hyp3_batch_factory(zip(ref_scenes, sec_scenes))
    landsat_pairs = gpd.GeoDataFrame({'reference': ref_scenes, 'secondary': sec_scenes, 'job_name': names})

    mock_get_authenticated_session.side_effect = [requests.Session()]
    mock_submit_prepared_jobs.side_effect = [landsat_jobs]
    jobs = hyp3.submit_pairs_for_processing(landsat_pairs)
    assert jobs == landsat_jobs
