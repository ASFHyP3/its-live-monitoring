"""Functions to support NISAR processing."""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import asf_search as asf
import geopandas as gpd
from asf_search.ASFProduct import ASFProduct

import config


log = logging.getLogger('its_live_monitoring')
log.setLevel(config.LOGGING_LEVEL)

NISAR_TILES_TO_PROCESS = json.loads((Path(__file__).parent / 'data' / 'nisar_tiles_to_process.json').read_text())
# FIXME: Which ones do we want to process?
NISAR_PRODUCTS_TO_PROCESS = ['RSLC', 'GSLC']
# NISAR_MIN_PAIR_SEPARATION_IN_DAYS = 12
NISAR_MAX_PAIR_SEPARATION_IN_DAYS = 183  # 6 months


def get_nisar_cmr_item(scene: str) -> ASFProduct:
    """Get the CMR Metadata for a given NISAR granule."""
    results = asf.product_search(scene)

    if len(results) == 0:
        raise ValueError(f'NISAR Burst {scene} could not be found')

    return results[0]


# TODO: Qualification criteria
#   - pols?
def product_qualifies_for_nisar_processing(product: ASFProduct, log_level: int = logging.DEBUG) -> bool:
    """Check if a NISAR product qualifies for processing."""
    scene = product.properties['sceneName']
    tile = '_'.join(scene.split('_')[5:8])
    instrument = scene.split('_')[1][0]

    if instrument != 'L':
        log.log(
            log_level,
            f'{scene} disqualifies for processing because it was not acquired with the L-band instrument',
        )
        return False

    if product.properties['processingLevel'] not in NISAR_PRODUCTS_TO_PROCESS:
        log.log(
            log_level,
            f'{scene} disqualifies for processing because it is not a supported product type: {NISAR_PRODUCTS_TO_PROCESS}',
        )
        return False

    if tile not in NISAR_TILES_TO_PROCESS:
        log.log(log_level, f'{scene} disqualifies for processing because it is not from a tile containing land-ice')
        return False

    log.log(log_level, f'{scene} qualifies for processing')
    return True


def get_nisar_pairs_for_reference_scene(
    reference: ASFProduct,
    *,
    max_pair_separation: int = NISAR_MAX_PAIR_SEPARATION_IN_DAYS,
) -> gpd.GeoDataFrame:
    """Generate potential ITS_LIVE velocity pairs for a given NISAR scene.

    Args:
        reference: The CMR metadata for a reference NISAR scene
        max_pair_separation: How many days back from a reference scene's acquisition date to start searching for
            secondary scenes

    Returns:
        A DataFrame with all potential pairs for a NISAR reference scene.
    """
    ref_name = reference.properties['sceneName']
    ref_date = datetime.fromisoformat(reference.properties['startTime'])

    start = ref_date - timedelta(days=max_pair_separation)
    end = ref_date - timedelta(seconds=1)

    stack = asf.search(
        dataset='NISAR',
        processingLevel=reference.properties['processingLevel'],
        relativeOrbit=reference.properties['pathNumber'],
        frame=reference.properties['frameNumber'],
        start=start,
        end=end,
    )

    tile = '_'.join(ref_name.split('_')[5:8])
    if len(stack) == 0:
        raise ValueError(f'No NISAR scenes found in {tile} stack.')

    secondaries = [product for product in stack if product_qualifies_for_nisar_processing(product)]
    log.debug(f'Found {len(secondaries)} secondary scenes for {ref_name}')
    if len(secondaries) == 0:
        return gpd.GeoDataFrame({'reference': [], 'reference_acquisition': [], 'secondary': [], 'job_name': []})

    pair_data = []
    for secondary in secondaries:
        pair_data.append(
            (
                (ref_name,),
                ref_date,
                (secondary.properties['sceneName'],),
                ref_name,
            )
        )

    return gpd.GeoDataFrame(pair_data, columns=['reference', 'reference_acquisition', 'secondary', 'job_name'])
