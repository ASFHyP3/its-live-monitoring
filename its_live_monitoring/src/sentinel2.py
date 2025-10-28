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
SENTINEL2_TILES_TO_PROCESS = json.loads(
    (Path(__file__).parent / 'data' / 'sentinel2_tiles_to_process.json').read_text()
)

SENTINEL2_MAX_PAIR_SEPARATION_IN_DAYS = 544
SENTINEL2_MIN_PAIR_SEPARATION_IN_DAYS = 5
SENTINEL2_MAX_CLOUD_COVER_PERCENT = 70
SENTINEL2_MIN_DATA_COVERAGE = 70

SESSION = requests.Session()

log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def raise_for_missing_in_google_cloud(scene_name: str) -> None:
    """Raises a 'requests.HTTPError' if the scene is not in Google Cloud yet.

    Args:
        scene_name: The scene to check for in Google Cloud.
    """
    root_url = 'https://storage.googleapis.com/gcp-public-data-sentinel-2/tiles'
    tile = f'{scene_name[39:41]}/{scene_name[41:42]}/{scene_name[42:44]}'

    manifest_url = f'{root_url}/{tile}/{scene_name}.SAFE/manifest.safe'
    response = requests.head(manifest_url)
    response.raise_for_status()


def get_data_coverage_for_item(item: pystac.Item) -> float:
    """Gets the percentage of the tile covered by valid data.

    Args:
        item: The desired stac item to add data coverage too.

    Returns:
        data_coverage: The data coverage percentage as a float.
    """
    tile_info_path = item.assets['tileinfo_metadata'].href.replace('s3://', 'https://roda.sentinel-hub.com/')

    response = SESSION.get(tile_info_path)
    response.raise_for_status()
    data_coverage = response.json()['dataCoveragePercentage']

    return data_coverage


def get_sentinel2_stac_item(scene: str) -> pystac.Item:
    """Retrieves a STAC item from the Sentinel-2 L1C Collection, throws ValueError if none found.

    Args:
        scene: The element84 scene name for the desired stac item.

    Returns:
        item: The desired stac item.
    """
    results = SENTINEL2_CATALOG.search(collections=[SENTINEL2_COLLECTION_NAME], query=[f's2:product_uri={scene}.SAFE'])
    items = [item for page in results.pages() for item in page]
    if (n_items := len(items)) != 1:
        raise ValueError(
            f'{n_items} items for {scene} found in Sentinel-2 STAC collection: '
            f'{SENTINEL2_CATALOG_API}/collections/{SENTINEL2_COLLECTION_NAME}'
        )
    item = items[0]
    return item


def is_new_scene(
    scene_name: str,
    log_level: int = logging.DEBUG,
) -> bool:
    """Determines whether a Sentinel-2 scene is new or part of a reprocessing campaign.

    Args:
        scene_name: Name of the Sentinel-2 scene
        log_level: The logging level

    Returns:
        A bool that is False if the scene is part of a reprocessing campaign and True otherwise

    """
    processing_baseline = scene_name.split('_')[3]
    if processing_baseline == 'N0500':
        # Reprocessing activity: https://sentinels.copernicus.eu/web/sentinel/technical-guides/sentinel-2-msi/copernicus-sentinel-2-collection-1-availability-status
        # Naming convention: https://sentinels.copernicus.eu/web/sentinel/user-guides/sentinel-2-msi/naming-convention
        # Processing baselines: https://sentinels.copernicus.eu/web/sentinel/technical-guides/sentinel-2-msi/processing-baseline
        log.log(
            log_level,
            f'{scene_name} disqualifies for processing because the processing baseline identifier '
            'indicates it is a product from a reprocessing activity',
        )
        return False
    return True


def qualifies_for_sentinel2_processing(
    item: pystac.Item,
    *,
    relative_orbit: str | None = None,
    max_cloud_cover: int = SENTINEL2_MAX_CLOUD_COVER_PERCENT,
    log_level: int = logging.DEBUG,
) -> bool:
    """Determines whether a scene is a valid Sentinel-2 product for processing.

    Args:
        item: STAC item of the desired Sentinel-2 scene.
        relative_orbit: scene must be from this relative orbit if provided.
        max_cloud_cover: The maximum allowable percentage of cloud cover.
        log_level: The logging level

    Returns:
        A bool that is True if the scene qualifies for Sentinel-2 processing, else False.
    """
    item_scene_id = item.properties['s2:product_uri'].removesuffix('.SAFE')

    if relative_orbit is not None:
        item_relative_orbit = item_scene_id.split('_')[4]
        if item_relative_orbit != relative_orbit:
            log.log(
                log_level,
                f'{item_scene_id} disqualifies for processing because its relative orbit ({item_relative_orbit}) '
                f'does not match the required relative orbit ({relative_orbit}).',
            )
            return False

    if item.collection_id != SENTINEL2_COLLECTION_NAME:
        log.log(log_level, f'{item_scene_id} disqualifies for processing because it is from the wrong collection')
        return False

    if not is_new_scene(item_scene_id, log_level):
        return False

    if not item.properties['s2:product_type'].endswith('1C'):
        log.log(log_level, f'{item_scene_id} disqualifies for processing because it is the wrong product type.')
        return False

    if 'msi' not in item.properties['instruments']:
        log.log(
            log_level,
            f'{item_scene_id} disqualifies for processing because it was not imaged with the right instrument',
        )
        return False

    grid_square = item.properties['grid:code']
    if grid_square not in SENTINEL2_TILES_TO_PROCESS:
        log.log(
            log_level,
            f'{item_scene_id} disqualifies for processing because it is not from a tile containing land-ice',
        )
        return False

    if item.properties.get('eo:cloud_cover', -1) < 0:
        log.log(log_level, f'{item_scene_id} disqualifies for processing because cloud coverage is unknown')
        return False

    if item.properties['eo:cloud_cover'] > max_cloud_cover:
        log.log(log_level, f'{item_scene_id} disqualifies for processing because it has too much cloud cover')
        return False

    if get_data_coverage_for_item(item) <= SENTINEL2_MIN_DATA_COVERAGE:
        log.log(log_level, f'{item_scene_id} disqualifies for processing because it has too little data coverage.')
        return False

    log.log(log_level, f'{item_scene_id} qualifies for processing')
    return True


def get_sentinel2_pairs_for_reference_scene(
    reference: pystac.Item,
    *,
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
    assert reference.collection_id is not None
    assert reference.datetime is not None
    results = SENTINEL2_CATALOG.search(
        collections=[reference.collection_id],
        query=[
            f'grid:code={reference.properties["grid:code"]}',
            f'eo:cloud_cover<={max_cloud_cover}',
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - min_pair_separation],
    )

    reference_scene_id = reference.properties['s2:product_uri'].removesuffix('.SAFE')
    reference_orbit = reference_scene_id.split('_')[4]
    items = [
        item
        for page in results.pages()
        for item in page
        if qualifies_for_sentinel2_processing(item, relative_orbit=reference_orbit, max_cloud_cover=max_cloud_cover)
    ]

    log.debug(f'Found {len(items)} secondary scenes for {reference_scene_id}')
    if len(items) == 0:
        return gpd.GeoDataFrame({'reference': [], 'reference_acquisition': [], 'secondary': [], 'job_name': []})

    features = []
    for item in items:
        feature = item.to_dict()
        feature['properties']['reference'] = (reference_scene_id,)
        feature['properties']['secondary'] = (item.properties['s2:product_uri'].removesuffix('.SAFE'),)
        feature['properties']['reference_acquisition'] = reference.datetime
        feature['properties']['job_name'] = reference_scene_id
        features.append(feature)

    df = gpd.GeoDataFrame.from_features(features)
    df['datetime'] = pd.to_datetime(df.datetime, format='ISO8601')

    return df
