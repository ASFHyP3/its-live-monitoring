"""Lambda function to trigger low-latency Sentinel-2 processing from newly acquired scenes."""

import argparse
import json
import logging
import os
import sys
from datetime import timedelta
from pathlib import Path

import geopandas as gpd
import hyp3_sdk as sdk
import pandas as pd
import pystac
import pystac_client

SENTINEL2_CATALOG_API = 'https://earth-search.aws.element84.com/v1'
SENTINEL2_CATALOG = pystac_client.Client.open(SENTINEL2_CATALOG_API)
SENTINEL2_COLLECTION = 'sentinel-2-l1c'
SENTINEL2_TILES_TO_PROCESS = json.loads((Path(__file__).parent / 'sentinel2_tiles_to_process.json').read_text())

MAX_PAIR_SEPARATION_IN_DAYS = 544
MAX_CLOUD_COVER_PERCENT = 60

log = logging.getLogger()
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def qualifies_for_sentinel2_processing(
    item: pystac.item.Item, max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT, log_level: int = logging.DEBUG
) -> bool:
    if item.collection_id != SENTINEL2_COLLECTION:
        log.log(log_level, f'{item.id} disqualifies for processing because it is from the wrong collection')
        return False

    if 'msi' not in item.properties['instruments']:
        log.log(log_level, f'{item.id} disqualifies for processing because it was not imaged with the right instrument')
        return False

    utm_zone = str(item.properties['mgrs:utm_zone'])
    latitude_band = item.properties['mgrs:latitude_band']
    grid_square = item.properties['mgrs:grid_square']
    tile_location = utm_zone + latitude_band + grid_square
    if tile_location not in SENTINEL2_TILES_TO_PROCESS:
        log.log(log_level, f'{item.id} disqualifies for processing because it is not from a tile containing land-ice')
        return False

    if item.properties['eo:cloud_cover'] > max_cloud_cover or item.properties['eo:cloud_cover'] < 0:
        log.log(log_level, f'{item.id} disqualifies for processing because it has too much cloud cover')
        return False

    log.log(log_level, f'{item.id} qualifies for processing')
    return True


def get_sentinel2_pairs_for_reference_scene(
    reference: pystac.item.Item,
    max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPARATION_IN_DAYS),
    max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT,
) -> gpd.GeoDataFrame:
    """Generate potential ITS_LIVE velocity pairs for a given Sentinel 2 scene.

    Args:
        reference: STAC item of the Sentinel 2 reference scene to find pairs for
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary scenes
        max_cloud_cover: The maximum percent of the secondary scene that can be covered by clouds

    Returns:
        A DataFrame with all potential pairs for a sentinel 2 reference scene. Metadata in the columns will be for the
        *secondary* scene unless specified otherwise.
    """
    results = SENTINEL2_CATALOG.search(
        collections=[reference.collection_id],
        query=[
            f'mgrs:utm_zone={reference.properties["mgrs:utm_zone"]}',
            f'mgrs:latitude_band={reference.properties["mgrs:latitude_band"]}',
            f'mgrs:grid_square={reference.properties["mgrs:grid_square"]}',
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - timedelta(seconds=1)],
    )

    items = [item for page in results.pages() for item in page if qualifies_for_sentinel2_processing(item, max_cloud_cover)]

    log.debug(f'Found {len(items)} secondary scenes for {reference.id}')
    if len(items) == 0:
        return gpd.GeoDataFrame()

    features = []
    for item in items:
        feature = item.to_dict()
        feature['properties']['referenceId'] = reference.id
        feature['properties']['reference'] = reference.properties['s2:product_uri'].split('.')[0]
        feature['properties']['reference_acquisition'] = reference.datetime
        feature['properties']['secondaryId'] = item.id
        feature['properties']['secondary'] = item.properties['s2:product_uri'].split('.')[0]
        features.append(feature)

    df = gpd.GeoDataFrame.from_features(features)
    df['datetime'] = pd.to_datetime(df.datetime)

    return df
