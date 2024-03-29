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

log = logging.getLogger()
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


def _qualifies_for_processing(
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

    if item.properties['view:off_nadir'] != 0:
        log.log(log_level, f'{item.id} disqualifies for processing because it is off-nadir')
        return False

    log.log(log_level, f'{item.id} qualifies for processing')
    return True


def _get_stac_item(scene: str) -> pystac.item.Item:
    collection = LANDSAT_CATALOG.get_collection(LANDSAT_COLLECTION)
    item = collection.get_item(scene)
    if item is None:
        raise ValueError(f'Scene {scene} not found in STAC catalog')
    return item


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
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - timedelta(seconds=1)],
    )
    items = [item for page in results.pages() for item in page if _qualifies_for_processing(item, max_cloud_cover)]

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


def deduplicate_hyp3_pairs(pairs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensure we don't submit duplicate jobs to HyP3.

    Search HyP3 jobs since the reference scene's acquisition date and remove already processed pairs

    Args:
         pairs: A GeoDataFrame containing *at least*  these columns: `reference`, `reference_acquisition`, and
          `secondary`.

    Returns:
         The pairs GeoDataFrame with any already submitted pairs removed.
    """
    jobs = HYP3.find_jobs(
        job_type='AUTORIFT',
        start=pairs.iloc[0].reference_acquisition,
        name=pairs.iloc[0].reference,
        user_id=EARTHDATA_USERNAME,
    )

    df = pd.DataFrame([job.job_parameters['granules'] for job in jobs], columns=['reference', 'secondary'])

    df = df.set_index(['reference', 'secondary'])
    pairs = pairs.set_index(['reference', 'secondary'])

    duplicates = df.loc[df.index.isin(pairs.index)]
    if len(duplicates) > 0:
        pairs = pairs.drop(duplicates.index)

    return pairs.reset_index()


def submit_pairs_for_processing(pairs: gpd.GeoDataFrame) -> sdk.Batch:  # noqa: D103
    prepared_jobs = []
    for reference, secondary in pairs[['reference', 'secondary']].itertuples(index=False):
        prepared_job = HYP3.prepare_autorift_job(reference, secondary, name=reference)

        if publish_bucket := os.environ.get('PUBLISH_BUCKET', ''):
            prepared_job['job_parameters']['publish_bucket'] = publish_bucket

        prepared_jobs.append(prepared_job)

    log.debug(prepared_jobs)

    jobs = sdk.Batch()
    for batch in sdk.util.chunk(prepared_jobs):
        jobs += HYP3.submit_prepared_jobs(batch)

    return jobs


def process_scene(
    scene: str,
    max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPARATION_IN_DAYS),
    max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT,
    submit: bool = True,
) -> sdk.Batch:
    """Trigger Landsat processing for a scene.

    Args:
        scene: Reference Landsat scene name to build pairs for.
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary
         scenes.
        max_cloud_cover: The maximum percent a Landsat scene can be covered by clouds.
        submit: Submit pairs to HyP3 for processing.

    Returns:
        Jobs submitted to HyP3 for processing.
    """
    reference = _get_stac_item(scene)

    if not _qualifies_for_processing(reference, max_cloud_cover, logging.INFO):
        return sdk.Batch()

    pairs = get_landsat_pairs_for_reference_scene(reference, max_pair_separation, max_cloud_cover)
    log.info(f'Found {len(pairs)} pairs for {scene}')
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        log.debug(pairs.loc[:, ['reference', 'secondary']])

    pairs = deduplicate_hyp3_pairs(pairs)
    log.info(f'Deduplicated pairs; {len(pairs)} remaining')
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        log.debug(pairs.loc[:, ['reference', 'secondary']])

    jobs = sdk.Batch()
    if submit:
        jobs += submit_pairs_for_processing(pairs)

    log.info(jobs)
    return jobs


def lambda_handler(event: dict, context: object) -> dict:
    """Landsat processing lambda function.

    Accepts an event with SQS records for newly ingested Landsat scenes and processes each scene.

    Args:
        event: The event dictionary that contains the parameters sent when this function is invoked.
        context: The context in which is function is called.

    Returns:
        AWS SQS batchItemFailures JSON response including messages that failed to be processed
    """
    batch_item_failures = []
    for record in event['Records']:
        try:
            body = json.loads(record['body'])
            message = json.loads(body['Message'])
            _ = process_scene(message['landsat_product_id'])
        except Exception:
            log.exception(f'Could not process message {record["messageId"]}')
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}


def main() -> None:
    """Command Line wrapper around `process_scene`."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('reference', help='Reference Landsat scene name to build pairs for')
    parser.add_argument(
        '--max-pair-separation',
        type=int,
        default=MAX_PAIR_SEPARATION_IN_DAYS,
        help="How many days back from a reference scene's acquisition date to search for secondary scenes",
    )
    parser.add_argument(
        '--max-cloud-cover',
        type=int,
        default=MAX_CLOUD_COVER_PERCENT,
        help='The maximum percent a Landsat scene can be covered by clouds',
    )
    parser.add_argument('--submit', action='store_true', help='Submit pairs to HyP3 for processing')
    parser.add_argument('-v', '--verbose', action='store_true', help='Turn on verbose logging')
    args = parser.parse_args()

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=level)
    log.debug(' '.join(sys.argv))

    _ = process_scene(args.reference, timedelta(days=args.max_pair_separation), args.max_cloud_cover, args.submit)


if __name__ == '__main__':
    main()
