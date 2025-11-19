from copy import deepcopy
from unittest.mock import patch

import pandas as pd
import pytest

import sentinel1


@patch('sentinel1.asf.granule_search')
def test_get_sentinel1_cmr_item(mock_asf_granule_search, asf_product_factory) -> None:
    scene_name = 'S1_247728_IW1_20251003T154900_VV_657C-BURST'
    full_burst_id = '116_247728_IW1'
    polarization = 'VV'
    start_time = '2025-10-03T15:49:00+00:00'
    expected_product = asf_product_factory(scene_name, full_burst_id, polarization, start_time)
    mock_asf_granule_search.side_effect = [[expected_product]]

    product = sentinel1.get_sentinel1_cmr_item('S1_247728_IW1_20251003T154900_VV_657C-BURST')
    assert product.properties['sceneName'] == scene_name
    assert product.properties['burst']['fullBurstID'] == full_burst_id
    assert product.properties['polarization'] == polarization
    assert product.properties['startTime'] == start_time


def test_product_qualifies_sentinel1_processing(asf_product_factory) -> None:
    scene_name = 'S1_247728_IW1_20251003T154900_VV_657C-BURST'
    full_burst_id = '116_247728_IW1'
    polarization = 'VV'
    start_time = '2025-10-03T15:49:00+00:00'
    good_product = asf_product_factory(scene_name, full_burst_id, polarization, start_time)

    assert sentinel1.product_qualifies_for_sentinel1_processing(good_product)

    product = deepcopy(good_product)
    product.properties['burst']['fullBurstID'] = 'foobar'
    assert not sentinel1.product_qualifies_for_sentinel1_processing(product)

    product = deepcopy(good_product)
    product.properties['polarization'] = 'HH'
    assert sentinel1.product_qualifies_for_sentinel1_processing(product)

    product = deepcopy(good_product)
    product.properties['polarization'] = 'HV'
    assert not sentinel1.product_qualifies_for_sentinel1_processing(product)


@patch('sentinel1.asf.search')
def test_get_frame_stacks(mock_asf_search, asf_product_factory, asf_stack_factory) -> None:
    scene_name = 'S1_247728_IW1_20251003T154900_VV_657C-BURST'
    full_burst_id = '116_247728_IW1'
    polarization = 'VV'
    start_time = '2025-10-03T15:49:00+00:00'
    reference = asf_product_factory(scene_name, full_burst_id, polarization, start_time)

    max_pair_seperation_in_days = 12
    expected_stacks = asf_stack_factory(
        scene_name, full_burst_id, days_seperation=range(0, max_pair_seperation_in_days + 1, 6)
    )
    mock_asf_search.side_effect = [*expected_stacks]

    df = sentinel1.get_frame_stacks(reference, max_pair_separation=max_pair_seperation_in_days)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 27 * 3
    assert df.frame_id.unique() == [30966]
    assert len(df.fullBurstID.unique()) == mock_asf_search.call_count

    mock_asf_search.side_effect = [*expected_stacks[:-1], []]
    with pytest.raises(ValueError, match=r'No bursts found in *'):
        sentinel1.get_frame_stacks(reference, max_pair_separation=max_pair_seperation_in_days)

    expected_stacks = asf_stack_factory(
        scene_name, full_burst_id, days_seperation=range(6, max_pair_seperation_in_days + 1, 6)
    )
    mock_asf_search.side_effect = [*expected_stacks]
    with pytest.raises(ValueError, match=r'No reference *'):
        sentinel1.get_frame_stacks(reference, max_pair_separation=max_pair_seperation_in_days)

    expected_stacks = asf_stack_factory(scene_name, full_burst_id, days_seperation=range(0, 1, 6))
    mock_asf_search.side_effect = [*expected_stacks]
    with pytest.raises(ValueError, match=r'No secondary *'):
        sentinel1.get_frame_stacks(reference, max_pair_separation=max_pair_seperation_in_days)


def test_frame_qualifies_for_sentinel1_processing() -> None:
    frame_id = 56
    df = pd.DataFrame(
        data=[
            '001_000443_IW1',
            '001_000444_IW1',
            '001_000445_IW1',
            '001_000446_IW1',
            '001_000447_IW1',
            '001_000448_IW1',
            '001_000449_IW1',
        ],
        columns=['fullBurstID'],
    )

    assert sentinel1.frame_qualifies_for_sentinel1_processing(df, frame_id)

    good_df = df.sample(5)
    assert sentinel1.frame_qualifies_for_sentinel1_processing(good_df, frame_id)

    bad_df = df.sample(4)
    assert not sentinel1.frame_qualifies_for_sentinel1_processing(bad_df, frame_id)


@patch('sentinel1.asf.search')
def test_get_sentinel1_pairs_for_reference_scene(mock_asf_search, asf_product_factory, asf_stack_factory) -> None:
    scene_name = 'S1_247728_IW1_20251003T154900_VV_657C-BURST'
    full_burst_id = '116_247728_IW1'
    polarization = 'VV'
    start_time = '2025-10-03T15:49:00+00:00'
    reference = asf_product_factory(scene_name, full_burst_id, polarization, start_time)

    max_pair_seperation_in_days = 12
    expected_stacks = asf_stack_factory(
        scene_name, full_burst_id, days_seperation=range(0, max_pair_seperation_in_days + 1, 6)
    )
    mock_asf_search.side_effect = [*expected_stacks]

    df = sentinel1.get_sentinel1_pairs_for_reference_scene(reference, max_pair_separation=max_pair_seperation_in_days)
    print(df.job_name.values)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert len(df.iloc[0].reference) == 27
    assert len(df.iloc[1].reference) == 27
    assert all(df.reference_acquisition == start_time)
    assert len(df.iloc[0].secondary) == 27
    assert len(df.iloc[1].secondary) == 27
    for job_name in df.job_name.values:
        assert job_name == f'OPERA_30966_{start_time}'
