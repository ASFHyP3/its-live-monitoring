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
SENTINEL1_MAX_PAIR_SEPARATION_IN_DAYS = 544


def get_sentinel1_cmr_item(scene: str) -> ASFProduct:
    """Get the CMR Metadata fora  given Sentinel-1 Burst granule."""
    results = asf.granule_search(scene)

    if len(results) == 0:
        raise ValueError(f'Sentinel-1 Burst {scene} could not be found')

    return results[0]


# FIXME: Is this really the only qualification criteria?
def qualifies_for_sentinel1_processing(product: ASFProduct, log_level: int = logging.DEBUG) -> bool:
    """Check if a Sentinel-1 Burst overlaps land-ice."""
    burst_id = product.properties['burst']['fullBurstID']
    if burst_id not in SENTINEL1_BURSTS_TO_PROCESS:
        log.log(log_level, f'{burst_id} disqualifies for processing because it is not from a burst containing land-ice')
        return False

    log.log(log_level, f'{burst_id} qualifies for processing')
    return True


def get_frame_stacks(
    reference: ASFProduct,
    *,
    max_pair_separation: int = SENTINEL1_MAX_PAIR_SEPARATION_IN_DAYS,
) -> tuple[pd.DataFrame, datetime]:
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

    frame_stacks = []
    for frame_id in frame_ids:
        burst_ids = OPERA_FRAMES_TO_BURST_IDS[str(frame_id)]

        burst_stacks = []
        for burst_id in burst_ids:
            stack = asf.search(fullBurstID=burst_id, start=start, end=end, polarization=polarization)

            if len(stack) == 0:
                raise ValueError(
                    f'Nu bursts found in {burst_id} stack for the OPERA frame that contains {reference_burst_id}.'
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

        burst_stacks = pd.concat(burst_stacks)
        burst_stacks['frame_id'] = frame_id
        frame_stacks.append(burst_stacks)

    df = pd.concat(frame_stacks)
    df['startTime'] = pd.to_datetime(df['startTime'])
    df['fullBurstID'] = df.burst.apply(lambda x: x['fullBurstID'])

    return df, ref_date


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
    df, ref_date = get_frame_stacks(reference, max_pair_separation=max_pair_separation)

    pair_data = []
    for frame in df.frame_id.unique():
        frames = list(df.loc[df.frame_id == frame].groupby(pd.Grouper(key='startTime', freq='12D', sort=True)))
        # pandas sorts earliest to latest
        ref_id, ref_products = frames[-1]
        for sec_id, sec_products in frames[:-1]:
            pair_data.append(
                (
                    ref_products.sceneName.tolist(),
                    sec_products.sceneName.tolist(),
                    f'OPERA_{frame}_{ref_date.date():%Y%m%d}_{sec_products.iloc[0].startTime.date():%Y%m%d}',
                )
            )

    return pd.DataFrame(pair_data, columns=['reference', 'secondary', 'name'])
