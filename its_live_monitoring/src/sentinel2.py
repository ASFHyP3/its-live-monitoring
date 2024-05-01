"""Lambda function to trigger low-latency Sentinel-2 processing from newly acquired scenes."""

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pystac
import pystac_client

from constants import MAX_CLOUD_COVER_PERCENT, MAX_PAIR_SEPARATION_IN_DAYS

SENTINEL2_CATALOG_API = 'https://catalogue.dataspace.copernicus.eu/stac'
SENTINEL2_CATALOG = pystac_client.Client.open(SENTINEL2_CATALOG_API)
SENTINEL2_COLLECTION = 'SENTINEL-2'
SENTINEL2_TILES_TO_PROCESS = json.loads((Path(__file__).parent / 'sentinel2_tiles_to_process.json').read_text())

log = logging.getLogger()
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def qualifies_for_sentinel2_processing(
    item: pystac.item.Item, max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT, log_level: int = logging.DEBUG
) -> bool:
    """Determines whether a scene is a valid Sentinel-2 product for processing.

    Args:
        item: STAC item of the desired Sentinel-2 scene
        max_cloud_cover: The maximum allowable percentage of cloud cover.
        log_level: The logging level

    Returns:
        A bool that is True if the scene qualifies for Sentinel-2 processing, else False.
    """
    if item.collection_id != SENTINEL2_COLLECTION:
        log.log(log_level, f'{item.id} disqualifies for processing because it is from the wrong collection')
        print("collection_id")
        return False

    if 'L1C' not in item.id:
        log.log(log_level, f'{item.id} disqualifies for processing because it is the wrong product type.')

    if 'MSI' not in item.properties['instrumentShortName']:
        log.log(log_level, f'{item.id} disqualifies for processing because it was not imaged with the right instrument')
        return False

    if item.properties['tileId'] not in SENTINEL2_TILES_TO_PROCESS:
        log.log(log_level, f'{item.id} disqualifies for processing because it is not from a tile containing land-ice')
        return False

    if item.properties.get('cloudCover', -1) < 0:
        log.log(log_level, f'{item.id} disqualifies for processing because cloud coverage is unknown')
        return False

    if item.properties['cloudCover'] > max_cloud_cover:
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
            f'tileId={reference.properties["tileId"]}'
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - timedelta(seconds=1)],
    )

    items = [
        item for page in results.pages() for item in page if qualifies_for_sentinel2_processing(item, max_cloud_cover)
    ]

    log.debug(f'Found {len(items)} secondary scenes for {reference.id}')
    if len(items) == 0:
        return gpd.GeoDataFrame({'reference': [], 'secondary': []})

    features = []
    for item in items:
        feature = item.to_dict()
        feature['properties']['reference'] = reference.id.split('.')[0]
        feature['properties']['reference_acquisition'] = reference.datetime
        feature['properties']['secondary'] = item.id.split('.')[0]
        features.append(feature)

    df = gpd.GeoDataFrame.from_features(features)
    df['datetime'] = pd.to_datetime(df.datetime)

    return df
