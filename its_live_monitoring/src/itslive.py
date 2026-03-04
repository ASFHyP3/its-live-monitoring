"""Functions for interacting with published ITS_LIVE products."""
from datetime import datetime

import geopandas as gpd
import pystac
import pystac_client

from sentinel1 import get_safe_acquisition_times


ITS_LIVE_CATALOG_API = 'https://stac.itslive.cloud/'
ITS_LIVE_CATALOG = pystac_client.Client.open(ITS_LIVE_CATALOG_API)
ITS_LIVE_COLLECTION_NAME = 'itslive-granules'
ITS_LIVE_COLLECTION = ITS_LIVE_CATALOG.get_collection(ITS_LIVE_COLLECTION_NAME)


def get_datetime(scene_name: str) -> datetime:
    """Get the acquisition start time from a Landsat, Sentinel-1 (SLC or Burst), or Sentinel-2 scene name."""
    if 'BURST' in scene_name:
        return datetime.strptime(scene_name[14:29], '%Y%m%dT%H%M%S')
    if scene_name.startswith('S1'):
        return datetime.strptime(scene_name[17:32], '%Y%m%dT%H%M%S')
    if scene_name.startswith('S2') and len(scene_name) > 25:  # ESA
        return datetime.strptime(scene_name[11:26], '%Y%m%dT%H%M%S')
    if scene_name.startswith('S2'):  # COG
        return datetime.strptime(scene_name.split('_')[2], '%Y%m%d')
    if scene_name.startswith('L'):
        return datetime.strptime(scene_name[17:25], '%Y%m%d')

    raise ValueError(f'Unsupported scene format: {scene_name}')


def sort_earliest_first(reference: str, secondary: str) -> tuple[str, str]:
    """Sort reference and secondary scene names according to the ITS_LIVE convention."""
    ref_datetime = get_datetime(reference)
    sec_datetime = get_datetime(secondary)

    if ref_datetime > sec_datetime:
        return secondary, reference

    return reference, secondary


def bursts_in_item(ref_datetime: datetime, sec_datetime: datetime, item: pystac.Item) -> bool:
    """Determines if the reference and secondary burst pairs fall within an ITS_LIVE granules.

    ITS_LIVE granules for Sentinel-1 report the synthetic burst2safe name for scene_1 (reference), scene_2 (secondary),
    so we must check and see if the bursts' acquisition start times falls with the start,stop time reported in the name.
    """
    scene_1, scene_2 = sort_earliest_first(item.properties['scene_1_id'], item.properties['scene_2_id'])

    scene_1_start, scene_1_stop = get_safe_acquisition_times(scene_1)
    scene_2_start, scene_2_stop = get_safe_acquisition_times(scene_2)

    # Bounds need to be inclusive because burst2safe just uses the first and last bursts datetimes
    if scene_1_start <= ref_datetime <= scene_1_stop and scene_2_start <= sec_datetime <= scene_2_stop:
        return True

    return False


def pair_exists(reference: str, secondary: str, name: str | None = None) -> bool:
    """Determine if a velocity granule for a scene pair has already been published to the ITS_LIVE STAC catalog."""
    reference, secondary = sort_earliest_first(reference, secondary)
    ref_datetime = get_datetime(reference)
    sec_datetime = get_datetime(secondary)

    if reference.startswith('S1'):
        frame = name.split('_')[1]
        results = ITS_LIVE_CATALOG.search(
            collections=[ITS_LIVE_COLLECTION_NAME],
            datetime=[ref_datetime, sec_datetime],
            filter={
                'op': 'and',
                'args': [
                    {'op': 'like', 'args': [{'property': 'platform'}, 'S1%']},
                    {'op': '=', 'args': [{'property': 'scene_1_frame'}, frame]},
                ],
            },
        )
        items = [item for page in results.pages() for item in page]
        items = [item for item in items if bursts_in_item(ref_datetime, sec_datetime, item)]

    else:
        results = ITS_LIVE_CATALOG.search(
            collections=[ITS_LIVE_COLLECTION_NAME],
            datetime=[ref_datetime, sec_datetime],
            query=[f'scene_1_id={reference}', f'scene_2_id={secondary}'],
        )
        items = [item for page in results.pages() for item in page]

    # This will effectively work just like `if items: return True`
    # but will put the reference scene message back into the DeadLetter queue so we can learn about and remove duplicates
    if (n_items := len(items)) > 1:
        raise ValueError(
            f'{n_items} items for ({reference}, {secondary}) found in ITS_LIVE STAC collection: '
            f'{ITS_LIVE_CATALOG_API}/collections/{ITS_LIVE_COLLECTION_NAME}'
        )

    if items:
        return True
    return False


def deduplicate_published_pairs(pairs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensures that pairs aren't submitted if they already have a product in S3.

    Args:
         pairs: A GeoDataFrame containing *at least*  these columns: `reference`, `reference_acquisition`, and
          `secondary`.

    Returns:
         The pairs GeoDataFrame with any already submitted pairs removed.
    """
    drop_indexes = []
    for idx, reference, secondary, name in pairs[['reference', 'secondary', 'job_name']].itertuples():
        if pair_exists(reference, secondary, name):
            drop_indexes.append(idx)

    return pairs.drop(index=drop_indexes)
