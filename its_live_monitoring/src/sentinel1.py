"""Functions to support Sentinel-1 processing."""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import asf_search as asf
import geopandas as gpd
import pandas as pd
from asf_search.ASFProduct import ASFProduct


log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))

OPERA_FRAMES_TO_BURST_IDS = json.loads((Path(__file__).parent / 'opera_frame_to_burst_ids.json').read_text())
BURST_IDS_TO_OPERA_FRAMES = json.loads((Path(__file__).parent / 'burst_id_to_opera_frame_ids.json').read_text())
SENTINEL1_BURSTS_TO_PROCESS = json.loads((Path(__file__).parent / 'sentinel1_tiles_to_process.json').read_text())
SENTINEL1_MAX_PAIR_SEPARATION_IN_DAYS = 12


def get_sentinel1_cmr_item(scene: str) -> ASFProduct:
    """Get the CMR Metadata fora  given Sentinel-1 Burst granule."""
    results = asf.granule_search(scene)

    if len(results) == 0:
        raise ValueError(f'Sentinel-1 Burst {scene} could not be found')

    return results[0]


def product_qualifies_for_sentinel1_processing(product: ASFProduct, log_level: int = logging.DEBUG) -> bool:
    """Check if a Sentinel-1 Burst product qualifies for processing."""
    burst_id = product.properties['burst']['fullBurstID']
    if burst_id not in SENTINEL1_BURSTS_TO_PROCESS:
        log.log(log_level, f'{burst_id} disqualifies for processing because it is not from a burst containing land-ice')
        return False

    if (polarization := product.properties['polarization']) not in [asf.constants.VV, asf.constants.HH]:
        log.log(log_level, f'{burst_id} disqualifies for processing because it has a {polarization} polarization')
        return False

    log.log(log_level, f'{burst_id} qualifies for processing')
    return True


def get_frame_stacks(
    reference: ASFProduct,
    *,
    max_pair_separation: int = SENTINEL1_MAX_PAIR_SEPARATION_IN_DAYS,
) -> pd.DataFrame:
    """Find all (if any) bursts for the OPERA frame(s) that the given reference burst is in.

    Args:
        reference: The CMR metadata for a reference Sentinel-1 Burst product
        max_pair_separation: How many days back from a reference scene's acquisition date to start searching for
            secondary scenes

    Returns:
        df: a DataFrame of every burst product in every from for as far back in time as the max pair seperation
    """
    reference_burst_id = reference.properties['burst']['fullBurstID']

    ref_date = datetime.fromisoformat(reference.properties['startTime'])
    start = ref_date - timedelta(days=max_pair_separation, minutes=3)
    end = ref_date + timedelta(minutes=3)
    polarization = reference.properties['sceneName'].split('_')[4]
    frame_ids = BURST_IDS_TO_OPERA_FRAMES[reference_burst_id]

    frame_stacks: list[pd.DataFrame] = []
    for frame_id in frame_ids:
        burst_ids = OPERA_FRAMES_TO_BURST_IDS[str(frame_id)]

        burst_stacks: list[pd.DataFrame] = []
        for burst_id in burst_ids:
            stack = asf.search(fullBurstID=burst_id, start=start, end=end, polarization=polarization)

            if len(stack) == 0:
                raise ValueError(
                    f'No bursts found in {burst_id} stack for the OPERA frame that contains {reference_burst_id}.'
                )

            if not ref_date - timedelta(minutes=3) < datetime.fromisoformat(stack[0].properties['startTime']):
                raise ValueError(
                    f'No reference {burst_id} burst product available for the OPERA frame that contains {reference_burst_id}.'
                )

            if len(stack) < 2:
                raise ValueError(
                    f'No secondary {burst_id} burst products available for the OPERA frame that contains {reference_burst_id}.'
                )

            burst_stacks.append(gpd.GeoDataFrame.from_features(stack.geojson()))

        all_burst_stacks: pd.DataFrame = pd.concat(burst_stacks)
        all_burst_stacks['frame_id'] = frame_id
        frame_stacks.append(all_burst_stacks)

    df: pd.DataFrame = pd.concat(frame_stacks)
    df['startTime'] = pd.to_datetime(df['startTime'])
    df['fullBurstID'] = df.burst.apply(lambda x: x['fullBurstID'])

    return df


def frame_qualifies_for_sentinel1_processing(frame: pd.DataFrame, frame_id: int) -> bool:
    """Check if an OPERA frame qualifies for processing."""
    expected_burst_ids = set(OPERA_FRAMES_TO_BURST_IDS[str(frame_id)])
    frame_burst_ids = set(frame.fullBurstID)
    if not frame_burst_ids.issubset(expected_burst_ids):
        log.debug(f'Burst IDs in OPERA frame {frame_id} not in expected burst IDs: {frame_burst_ids - expected_burst_ids}')
        return False

    if len(frame) < 5:
        log.debug(f'Not enough Burst IDs in OPERA frame {frame_id}: {frame}')
        return False

    log.debug(f'Frame {frame_id} qualifies for processing.')
    return True


def get_sentinel1_pairs_for_reference_scene(
    reference: ASFProduct,
    *,
    max_pair_separation: int = SENTINEL1_MAX_PAIR_SEPARATION_IN_DAYS,
) -> pd.DataFrame:
    """Generate potential ITS_LIVE velocity pairs for a given Sentinel-1 scene.

    Args:
        reference: The CMR metadata for a reference Sentinel-1 Burst product
        max_pair_separation: How many days back from a reference scene's acquisition date to start searching for
            secondary scenes

    Returns:
        DataFrame of pairs, which includes the burst scenes in the reference and secondary frames, and a name for each pair
    """
    df = get_frame_stacks(reference, max_pair_separation=max_pair_separation)

    pair_data = []
    for frame in df.frame_id.unique():
        frames = list(df.loc[df.frame_id == frame].groupby(pd.Grouper(key='startTime', freq='D', sort=True)))
        # pandas sorts earliest to latest
        ref_id, ref_products = frames[-1]
        ref_date = ref_products.startTime.min()

        assert frame_qualifies_for_sentinel1_processing(ref_products, frame_id=frame)

        for sec_id, sec_products in frames[:-1]:
            if frame_qualifies_for_sentinel1_processing(sec_products, frame_id=frame):
                pair_data.append(
                    (
                        tuple(ref_products.sceneName),
                        ref_date,
                        tuple(sec_products.sceneName),
                        f'OPERA_{frame}_{ref_date.isoformat()}',
                    )
                )

    return pd.DataFrame(pair_data, columns=['reference', 'reference_acquisition', 'secondary', 'job_name'])
