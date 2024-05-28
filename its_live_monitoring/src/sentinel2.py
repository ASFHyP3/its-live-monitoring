"""Functions to support Sentinel-2 processing."""

import json
import logging
import os
from datetime import timedelta
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pystac
import pystac_client
import requests


SENTINEL2_CATALOG_API = 'https://earth-search.aws.element84.com/v1/'
SENTINEL2_CATALOG = pystac_client.Client.open(SENTINEL2_CATALOG_API)
SENTINEL2_COLLECTION_NAME = 'sentinel-2-l1c'
SENTINEL2_COLLECTION = SENTINEL2_CATALOG.get_collection(SENTINEL2_COLLECTION_NAME)
SENTINEL2_TILES_TO_PROCESS = json.loads((Path(__file__).parent / 'sentinel2_tiles_to_process.json').read_text())

SENTINEL2_MAX_PAIR_SEPARATION_IN_DAYS = 544
SENTINEL2_MIN_PAIR_SEPARATION_IN_DAYS = 5
SENTINEL2_MAX_CLOUD_COVER_PERCENT = 70
SENTINEL2_MIN_DATA_COVERAGE = 70

log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def raise_for_missing_in_google_cloud(scene_name: str) -> None:  # noqa: D103
    root_url = 'https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles'
    tile = f'{scene_name[39:41]}/{scene_name[41:42]}/{scene_name[42:44]}'

    manifest_url = f'{root_url}/{tile}/{scene_name}.SAFE/manifest.safe'
    response = requests.head(manifest_url)
    response.raise_for_status()


def add_data_coverage_to_item(item: pystac.Item) -> pystac.Item:  # noqa: D103
    tile_info_path = item.assets['tileinfo_metadata'].href[5:]

    response = requests.get(f'https://roda.sentinel-hub.com/{tile_info_path}')
    response.raise_for_status()

    item.properties['s2:data_coverage'] = response.json()['dataCoveragePercentage']
    return item


def get_sentinel2_stac_item(scene: str) -> pystac.Item:  # noqa: D103
    results = SENTINEL2_CATALOG.search(collections=[SENTINEL2_COLLECTION_NAME], query=[f's2:product_uri={scene}.SAFE'])

    items = [item for page in results.pages() for item in page]

    if (n_items := len(items)) != 1:
        raise ValueError(
            f'{n_items} for {scene} found in Sentinel-2 STAC collection: '
            f'{SENTINEL2_CATALOG_API}/collections/{SENTINEL2_COLLECTION_NAME}'
        )

    item = items[0]
    item = add_data_coverage_to_item(item)

    return item


def qualifies_for_sentinel2_processing(
    item: pystac.Item,
    max_cloud_cover: int = SENTINEL2_MAX_CLOUD_COVER_PERCENT,
    log_level: int = logging.DEBUG,
) -> bool:
    """Determines whether a scene is a valid Sentinel-2 product for processing.

    Args:
        item: STAC item of the desired Sentinel-2 scene.
        max_cloud_cover: The maximum allowable percentage of cloud cover.
        log_level: The logging level

    Returns:
        A bool that is True if the scene qualifies for Sentinel-2 processing, else False.
    """
    if item.collection_id != SENTINEL2_COLLECTION_NAME:
        log.log(log_level, f'{item.id} disqualifies for processing because it is from the wrong collection')
        return False

    if item.properties['s2:product_uri'].split('_')[3] == 'N0500':
        # Reprocessing activity: https://sentinels.copernicus.eu/web/sentinel/technical-guides/sentinel-2-msi/copernicus-sentinel-2-collection-1-availability-status
        # Naming convention: https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/naming-convention
        # Processing baselines: https://sentinels.copernicus.eu/web/sentinel/technical-guides/sentinel-2-msi/processing-baseline
        log.log(
            log_level,
            f'{item.id} disqualifies for processing because the processing baseline identifier '
            f'indicates it is a product from a reprocessing activity',
        )
        return False

    product_type = item.properties['s2:product_uri'].split('_')[3]
    if not product_type.endswith('L1C'):
        log.log(log_level, f'{item.id} disqualifies for processing because it is the wrong product type.')
        return False

    if not product_type.startswith('MSI'):
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
    reference: pystac.Item,
    max_pair_separation: timedelta = timedelta(days=SENTINEL2_MAX_PAIR_SEPARATION_IN_DAYS),
    min_pair_separation: timedelta = timedelta(days=SENTINEL2_MIN_PAIR_SEPARATION_IN_DAYS),
    max_cloud_cover: int = SENTINEL2_MAX_CLOUD_COVER_PERCENT,
) -> gpd.GeoDataFrame:
    """Generate potential ITS_LIVE velocity pairs for a given Sentinel-2 scene.

    Args:
        reference: STAC item of the Sentinel-2 reference scene to find secondary scenes for
        max_pair_separation: How many days back from a reference scene's acquisition date to start searching for
            secondary scenes
        min_pair_separation: How many days back from a reference scene's acquisition date to stop searching for
            secondary scenes
        max_cloud_cover: The maximum percent of the secondary scene that can be covered by clouds

    Returns:
        A DataFrame with all potential pairs for a sentinel-2 reference scene. Metadata in the columns will be for the
        *secondary* scene unless specified otherwise.
    """
    results = SENTINEL2_CATALOG.search(
        collections=[reference.collection_id],
        query=[
            f'grid:code={reference.properties["grid:code"]}',
            f'eo:cloud_cover<={SENTINEL2_MAX_CLOUD_COVER_PERCENT}',
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - min_pair_separation],
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
        feature['properties']['reference'] = reference.properties['s2:product_uri'].rstrip('.SAFE')
        feature['properties']['reference_acquisition'] = reference.datetime
        feature['properties']['secondary'] = item.properties['s2:product_uri'].rstrip('.SAFE')
        features.append(feature)

    df = gpd.GeoDataFrame.from_features(features)
    df['datetime'] = pd.to_datetime(df.datetime)

    return df
