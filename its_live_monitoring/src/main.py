"""Lambda function to trigger low-latency Landsat and Sentinel-2 processing from newly acquired scenes."""

import argparse
import json
import logging
import os
import sys
from datetime import timedelta

import geopandas as gpd
import hyp3_sdk as sdk
import pandas as pd

from constants import MAX_CLOUD_COVER_PERCENT, MAX_PAIR_SEPARATION_IN_DAYS, MIN_DATA_COVER_PERCENT
from landsat import (
    get_landsat_pairs_for_reference_scene,
    get_landsat_stac_item,
    qualifies_for_landsat_processing,
)
from sentinel2 import (
    get_sentinel2_pairs_for_reference_scene,
    get_sentinel2_stac_item,
    qualifies_for_sentinel2_processing,
    raise_for_missing_in_google_cloud,
)


EARTHDATA_USERNAME = os.environ.get('EARTHDATA_USERNAME')
EARTHDATA_PASSWORD = os.environ.get('EARTHDATA_PASSWORD')
HYP3 = sdk.HyP3(
    os.environ.get('HYP3_API', 'https://hyp3-its-live.asf.alaska.edu'),
    username=EARTHDATA_USERNAME,
    password=EARTHDATA_PASSWORD,
)

log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


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


def process_scene(scene: str,
                  max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPARATION_IN_DAYS),
                  max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT,
                  min_data_cover: int = MIN_DATA_COVER_PERCENT,
                  s2_tile_path: str = None,
                  submit: bool = True,) -> sdk.Batch:
    """Trigger Landsat processing for a scene.

    Args:
        scene: Reference Landsat scene name to build pairs for.
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary
         scenes.
        max_cloud_cover: The maximum percent a Landsat scene can be covered by clouds.
        min_data_cover: The minimum percent of data coverage of the sentinel-2 scene
        s2_tile_path: The path for the tile of the sentinel2 scene.
        submit: Submit pairs to HyP3 for processing.

    Returns:
        Jobs submitted to HyP3 for processing.
    """
    pairs = None
    if scene.startswith('S2'):
        reference = get_sentinel2_stac_item(f'{scene}.SAFE')
        if qualifies_for_sentinel2_processing(reference, s2_tile_path, max_cloud_cover, min_data_cover, logging.INFO):
            # hyp3-its-live will pull scenes from Google Cloud; ensure the new scene is there before processing
            # Note: Time between attempts is controlled by they SQS VisibilityTimout
            _ = raise_for_missing_in_google_cloud(scene)
            pairs = get_sentinel2_pairs_for_reference_scene(reference, max_pair_separation, max_cloud_cover)

    else:
        reference = get_landsat_stac_item(scene)
        if qualifies_for_landsat_processing(reference, max_cloud_cover, logging.INFO):
            pairs = get_landsat_pairs_for_reference_scene(reference, max_pair_separation, max_cloud_cover)

    if pairs is None:
        return sdk.Batch()

    log.info(f'Found {len(pairs)} pairs for {scene}')
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        log.debug(pairs.sort_values(by=['secondary'], ascending=False).loc[:, ['reference', 'secondary']])

    if len(pairs) > 0:
        pairs = deduplicate_hyp3_pairs(pairs)

        log.info(f'Deduplicated pairs; {len(pairs)} remaining')
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
            log.debug(pairs.sort_values(by=['secondary'], ascending=False).loc[:, ['reference', 'secondary']])

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
            product_id = 'landsat_product_id' if 'landsat_product_id' in message.keys() else 'name'

            s2_tile_path = None
            if message[product_id].startswith('S2'):
                s2_tile_path = message['tiles'][0]['path']

            _ = process_scene(message[product_id], s2_tile_path=s2_tile_path)
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

    logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.debug(' '.join(sys.argv))
    _ = process_scene(args.reference, timedelta(days=args.max_pair_separation), args.max_cloud_cover, args.submit)


if __name__ == '__main__':
    main()
