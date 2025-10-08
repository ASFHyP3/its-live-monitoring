"""Functions to support Landsat processing."""

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pystac
import pystac_client


LANDSAT_CATALOG_API = 'https://landsatlook.usgs.gov/stac-server'
LANDSAT_CATALOG = pystac_client.Client.open(LANDSAT_CATALOG_API)
LANDSAT_COLLECTION_NAME = 'landsat-c2l1'
LANDSAT_COLLECTION = LANDSAT_CATALOG.get_collection(LANDSAT_COLLECTION_NAME)
LANDSAT_TILES_TO_PROCESS = json.loads((Path(__file__).parent / 'landsat_tiles_to_process.json').read_text())

LANDSAT_MAX_PAIR_SEPARATION_IN_DAYS = 544
LANDSAT_MAX_CLOUD_COVER_PERCENT = 60

log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def get_landsat_stac_item(scene: str) -> pystac.Item:  # noqa: D103
    item = LANDSAT_COLLECTION.get_item(scene)
    if item is None:
        raise ValueError(
            f'Scene {scene} not found in Landsat STAC collection: '
            f'{LANDSAT_CATALOG_API}/collections/{LANDSAT_COLLECTION_NAME}'
        )
    return item


def qualifies_for_landsat_processing(
    item: pystac.item.Item, *, max_cloud_cover: int = LANDSAT_MAX_CLOUD_COVER_PERCENT, log_level: int = logging.DEBUG
) -> bool:
    """Determines whether a scene is a valid Landsat product for processing.

    Args:
        item: STAC item of the desired Landsat scene
        max_cloud_cover: The maximum allowable percentage of cloud cover.
        log_level: The logging level

    Returns:
        A bool that is True if the scene qualifies for Landsat processing, else False.
    """
    if item.collection_id != LANDSAT_COLLECTION_NAME:
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
    *,
    max_pair_separation: timedelta = timedelta(days=LANDSAT_MAX_PAIR_SEPARATION_IN_DAYS),
    max_cloud_cover: int = LANDSAT_MAX_CLOUD_COVER_PERCENT,
) -> gpd.GeoDataFrame:
    """Generate potential ITS_LIVE velocity pairs for a given Landsat scene.

    Args:
        reference: STAC item of the Landsat reference scene to find pairs for
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary scenes
        max_cloud_cover: The maximum percent of the secondary scene that can be covered by clouds

    Returns:
        A DataFrame with all potential pairs for a Landsat reference scene. Metadata in the columns will be for the
        *secondary* scene unless specified otherwise.
    """
    assert reference.collection_id is not None
    assert reference.datetime is not None
    results = LANDSAT_CATALOG.search(
        collections=[reference.collection_id],
        query=[
            f'landsat:wrs_path={reference.properties["landsat:wrs_path"]}',
            f'landsat:wrs_row={reference.properties["landsat:wrs_row"]}',
            'view:off_nadir>0' if reference.properties['view:off_nadir'] > 0 else 'view:off_nadir=0',
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - timedelta(seconds=1)],
    )

    items = [
        item
        for page in results.pages()
        for item in page
        if qualifies_for_landsat_processing(item, max_cloud_cover=max_cloud_cover)
    ]

    log.debug(f'Found {len(items)} secondary scenes for {reference.id}')
    if len(items) == 0:
        return gpd.GeoDataFrame({'reference': [], 'secondary': []})

    features = []
    for item in items:
        feature = item.to_dict()
        feature['properties']['reference'] = (reference.id,)
        feature['properties']['secondary'] = (item.id,)
        feature['properties']['reference_acquisition'] = reference.datetime
        feature['properties']['job_name'] = reference.id
        features.append(feature)

    df = gpd.GeoDataFrame.from_features(features)
    df['datetime'] = pd.to_datetime(df.datetime, format='ISO8601')

    return df
