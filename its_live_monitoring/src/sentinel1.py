"""Functions to support Sentinel-1 processing."""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import asf_search


log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))

OPERA_FRAMES_TO_BURST_IDS = json.loads((Path(__file__).parent / 'opera_frame_to_burst_ids.json').read_text())
BURST_IDS_TO_OPERA_FRAMES = json.loads((Path(__file__).parent / 'burst_id_to_opera_frame_ids.json').read_text())
SENTINEL1_BURSTS_TO_PROCESS = json.loads((Path(__file__).parent / 'sentinel1_tiles_to_process.json').read_text())
SENTINEL1_MAX_PAIR_SEPARATION_IN_DAYS = 544
SENTINEL1_MIN_PAIR_SEPARATION_IN_DAYS = 5


def get_burst_id(granule: str) -> str:
    """Get the OPERA Burst ID for a given Sentinel-1 Burst."""
    res = asf_search.granule_search(granule)
    return res[0].get_stack_opts().fullBurstID[0]


def qualifies_for_sentinel1_processing(scene: str) -> bool:
    """Check if a Sentinel-1 Burst overlaps land-ice."""
    burst_id = get_burst_id(scene)
    if burst_id in SENTINEL1_BURSTS_TO_PROCESS:
        return True
    return False


def check_frame(granule: str) -> tuple[list, list, list, datetime]:
    """Find all (if any) reference and secondary bursts for the OPERA frame(s) that the given burst is in.

    Args:
        granule: The Sentinel-1 Burst granule name

    Returns:
        list containing lists of reference bursts corresponding to OPERA frames
        list containing lists of secondary bursts corresponding to the reference OPERA frames
        list of the OPERA frame ids
        the reference aquisition date
    """
    reference = asf_search.granule_search(granule)[0]

    if not reference:
        raise ValueError(f'Reference Sentinel-1 granule {granule} could not be found')

    burst_id = reference.get_stack_opts().fullBurstID[0]
    ref_date = datetime.strptime(reference.properties['startTime'], '%Y-%m-%dT%H:%M:%SZ')
    start = ref_date - timedelta(minutes=3)
    end = start + timedelta(minutes=6)
    polarization = granule.split('_')[4]
    frame_ids = BURST_IDS_TO_OPERA_FRAMES[burst_id]

    burst_ids = []
    for f_id in frame_ids:
        burst_ids.append(OPERA_FRAMES_TO_BURST_IDS[str(f_id)])

    bursts = []
    for b_ids in burst_ids:
        burst_subset = []
        for b_id in b_ids:
            response = asf_search.search(fullBurstID=b_id, start=start, end=end, polarization=polarization)

            if len(response) == 0:
                raise ValueError(f'Not all bursts are available for the OPERA frame that contains {reference}.')

            assert len(response) == 1

            burst_subset.append(response[0].properties['sceneName'])
        bursts.append(burst_subset)

    return bursts, burst_ids, frame_ids, ref_date


def get_sentinel1_pairs_for_reference_scene(
    reference: str,
    *,
    max_pair_separation: timedelta = timedelta(days=SENTINEL1_MAX_PAIR_SEPARATION_IN_DAYS),
    min_pair_separation: timedelta = timedelta(days=SENTINEL1_MIN_PAIR_SEPARATION_IN_DAYS),
) -> tuple[list, list, list]:
    """Generate potential ITS_LIVE velocity pairs for a given Sentinel-1 scene.

    Args:
        reference: reference Sentinel-1 granule name
        max_pair_separation: How many days back from a reference scene's acquisition date to start searching for
            secondary scenes
        min_pair_separation: How many days back from a reference scene's acquisition date to stop searching for
            secondary scenes

    Returns:
        list of reference frames containing the reference scenes for 1 or 2 Opera frames
        list of secondary frames continaing the secondary scenes for 1 or 2 Opera frames
        list of job names
    """
    polarization = reference.split('_')[4]
    references, burst_id_group, frame_ids, ref_date = check_frame(granule=reference)

    secondaries = []
    secondary_dates = []
    for burst_ids in burst_id_group:
        secondary_subset = []
        secondary_frame_dates: list[datetime] = []
        for burst_id in burst_ids:
            results = asf_search.search(
                platform='S1',
                start=ref_date - max_pair_separation,
                end=ref_date - min_pair_separation,
                fullBurstID=burst_id,
                polarization=polarization,
            )
            assert len(results) >= 1

            if secondary_frame_dates == []:
                secondary_frame_dates = [
                    datetime.strptime(result.properties['startTime'], '%Y-%m-%dT%H:%M:%SZ') for result in results
                ]

            secondary_subset.append([result.properties['sceneName'] for result in results])
        secondary_dates.append(secondary_frame_dates)
        secondaries.append(list(zip(*secondary_subset)))

    frame_job_names = []
    for frame_id, sec_dates in zip(frame_ids, secondary_dates):
        job_names = []
        for sec_date in sec_dates:
            job_names.append(f'OPERA_{frame_id}_{ref_date}_{sec_date}')
        frame_job_names.append(job_names)

    return references, secondaries, frame_job_names
