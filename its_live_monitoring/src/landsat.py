"""Lambda function to trigger low-latency Landsat processing from newly acquired scenes."""

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

LANDSAT_STAC_API = 'https://landsatlook.usgs.gov/stac-server'
LANDSAT_CATALOG = pystac_client.Client.open(LANDSAT_STAC_API)
LANDSAT_COLLECTION = 'landsat-c2l1'
LANDSAT_TILES_TO_PROCESS = json.loads((Path(__file__).parent / 'landsat_tiles_to_process.json').read_text())

MAX_PAIR_SEPARATION_IN_DAYS = 544
MAX_CLOUD_COVER_PERCENT = 60

EARTHDATA_USERNAME = os.environ.get('EARTHDATA_USERNAME')
EARTHDATA_PASSWORD = os.environ.get('EARTHDATA_PASSWORD')
HYP3 = sdk.HyP3(
    os.environ.get('HYP3_API', 'https://hyp3-its-live.asf.alaska.edu'),
    username=EARTHDATA_USERNAME,
    password=EARTHDATA_PASSWORD,
)

log = logging.getLogger(__name__)
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def qualifies_for_landsat_processing(
    item: pystac.item.Item, max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT, log_level: int = logging.DEBUG
) -> bool:
    if item.collection_id != 'landsat-c2l1':
        log.log(log_level, f'{item.id} disqualifies for processing because it is from the wrong collection')
        return False

    if 'OLI' not in item.properties['instruments']:
        log.log(log_level, f'{item.id} disqualifies for processing because it was not imaged with the right instrument')
        return False

    if item.properties['landsat:collection_category'] not in ['T1', 'T2']:
        log.log(log_level, f'{item.id} disqualifies for processing because it is from the wrong tier')
        return False

    if item.properties['landsat:wrs_path'] + item.properties['landsat:wrs_row'] not in LANDSAT_TILES_TO_PROCESS:
        log.log(log_level, f'{item.id} disqualifies for processing because it is not from a tile containing land-ice')
        return False

    if item.properties.get('landsat:cloud_cover_land', -1) < 0:
        log.log(log_level, f'{item.id} disqualifies for processing because cloud coverage is unknown')
        return False

    if item.properties['landsat:cloud_cover_land'] > max_cloud_cover:
        log.log(log_level, f'{item.id} disqualifies for processing because it has too much cloud cover')
        return False

    log.log(log_level, f'{item.id} qualifies for processing')
    return True


def get_landsat_pairs_for_reference_scene(
    reference: pystac.item.Item,
    max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPARATION_IN_DAYS),
    max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT,
) -> gpd.GeoDataFrame:
    """Generate potential ITS_LIVE velocity pairs for a given Landsat scene.

    Args:
        reference: STAC item of the Landsat reference scene to find pairs for
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary scenes
        max_cloud_cover: The maximum percent of the secondary scene that can be covered by clouds

    Returns:
        A DataFrame with all potential pairs for a landsat reference scene. Metadata in the columns will be for the
        *secondary* scene unless specified otherwise.
    """
    results = LANDSAT_CATALOG.search(
        collections=[reference.collection_id],
        query=[
            f'landsat:wrs_path={reference.properties["landsat:wrs_path"]}',
            f'landsat:wrs_row={reference.properties["landsat:wrs_row"]}',
            'view:off_nadir>0' if reference.properties['view:off_nadir'] > 0 else 'view:off_nadir=0',
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - timedelta(seconds=1)],
    )

    items = [item for page in results.pages() for item in page if qualifies_for_landsat_processing(item, max_cloud_cover)]

    log.debug(f'Found {len(items)} secondary scenes for {reference.id}')
    if len(items) == 0:
        return gpd.GeoDataFrame({'reference': [], 'secondary': []})

    features = []
    for item in items:
        feature = item.to_dict()
        feature['properties']['reference'] = reference.id
        feature['properties']['reference_acquisition'] = reference.datetime
        feature['properties']['secondary'] = item.id
        features.append(feature)

    df = gpd.GeoDataFrame.from_features(features)
    df['datetime'] = pd.to_datetime(df.datetime)

    return df
