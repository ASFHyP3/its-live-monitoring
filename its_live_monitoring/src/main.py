"""Lambda function to trigger low-latency Landsat and Sentinel-2 processing from newly acquired scenes."""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

import boto3
import geopandas as gpd
import hyp3_sdk as sdk
import numpy as np
import pandas as pd
from boto3.dynamodb.conditions import Attr, Key

from landsat import (
    get_landsat_pairs_for_reference_scene,
    get_landsat_stac_item,
    qualifies_for_landsat_processing,
)
from sentinel2 import (
    get_sentinel2_pairs_for_reference_scene,
    get_sentinel2_stac_item,
    is_new_scene,
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

s3 = boto3.client('s3')
dynamo = boto3.resource('dynamodb')


def point_to_region(lat: float, lon: float) -> str:
    """Returns a string (for example, N78W124) of a region name based on granule center point lat,lon."""
    nw_hemisphere = 'S' if np.signbit(lat) else 'N'
    ew_hemisphere = 'W' if np.signbit(lon) else 'E'

    region_lat = int(np.abs(np.fix(lat / 10) * 10))
    if region_lat == 90:  # if you are exactly at a pole, put in lat = 80 bin
        region_lat = 80

    region_lon = int(np.abs(np.fix(lon / 10) * 10))
    if region_lon >= 180:  # if you are at the dateline, back off to the 170 bin
        region_lon = 170

    return f'{nw_hemisphere}{region_lat:02d}{ew_hemisphere}{region_lon:03d}'


def regions_from_bounds(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> set[str]:
    """Returns a set of all region names within a bounding box."""
    lats, lons = np.mgrid[min_lat : max_lat + 10 : 10, min_lon : max_lon + 10 : 10]
    return {point_to_region(lat, lon) for lat, lon in zip(lats.ravel(), lons.ravel())}


def get_key(tile_prefixes: list[str], reference: str, secondary: str) -> str | None:
    """Search S3 for the key of a processed pair.

    Args:
        tile_prefixes: s3 tile path prefixes
        reference: reference scene name
        secondary: secondary scene name

    Returns:
        The key or None if one wasn't found.
    """
    # NOTE: hyp3-autorift enforces earliest scene as the reference scene and will write files accordingly,
    #       but its-live-monitoring uses the latest scene as the reference scene, so enforce autorift convention
    reference, secondary = sorted([reference, secondary])

    for tile_prefix in tile_prefixes:
        prefix = f'{tile_prefix}/{reference}_X_{secondary}'
        response = s3.list_objects_v2(
            Bucket=os.environ.get('PUBLISH_BUCKET', 'its-live-data'),
            Prefix=prefix,
        )
        for item in response.get('Contents', []):
            if item['Key'].endswith('.nc'):
                return item['Key']
    return None


def deduplicate_s3_pairs(pairs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Ensures that pairs aren't submitted if they already have a product in S3.

    Args:
         pairs: A GeoDataFrame containing *at least*  these columns: `reference`, `reference_acquisition`, and
          `secondary`.

    Returns:
         The pairs GeoDataFrame with any already submitted pairs removed.
    """
    s2_prefix = 'velocity_image_pair/sentinel2/v02'
    landsat_prefix = 'velocity_image_pair/landsatOLI/v02'
    prefix = s2_prefix if pairs['reference'][0].startswith('S2') else landsat_prefix

    regions = regions_from_bounds(*pairs['geometry'].total_bounds)
    tile_prefixes = [f'{prefix}/{region}' for region in regions]

    drop_indexes = []
    for idx, reference, secondary in pairs[['reference', 'secondary']].itertuples():
        if get_key(tile_prefixes=tile_prefixes, reference=reference, secondary=secondary):
            drop_indexes.append(idx)

    return pairs.drop(index=drop_indexes)


def format_time(time: datetime) -> str:
    """Format time to ISO with UTC timezone.

    Args:
        time: a datetime object to format

    Returns:
        datetime: the UTC time in ISO format
    """
    if time.tzinfo is None:
        raise ValueError(f'missing tzinfo for datetime {time}')
    utc_time = time.astimezone(timezone.utc)
    return utc_time.isoformat(timespec='seconds')


def query_jobs_by_status_code(status_code: str, user: str, name: str, start: datetime) -> sdk.Batch:
    """Query dynamodb for jobs by status_code, then filter by user, name, and date.

    Args:
        status_code: `status_code` of the desired jobs
        user: the `user_id` that submitted the jobs
        name: the name of the jobs
        start: the earliest submission date of the jobs

    Returns:
        sdk.Batch: batch of jobs matching the filters
    """
    table = dynamo.Table(os.environ['JOBS_TABLE_NAME'])

    key_expression = Key('status_code').eq(status_code)

    filter_expression = Attr('user_id').eq(user) & Attr('name').eq(name) & Attr('request_time').gte(format_time(start))

    params = {
        'IndexName': 'status_code',
        'KeyConditionExpression': key_expression,
        'FilterExpression': filter_expression,
        'ScanIndexForward': False,
    }

    jobs = []
    while True:
        response = table.query(**params)
        jobs.extend(response['Items'])
        if (next_key := response.get('LastEvaluatedKey')) is None:
            break
        params['ExclusiveStartKey'] = next_key

    return sdk.Batch([sdk.Job.from_dict(job) for job in jobs])


def deduplicate_hyp3_pairs(pairs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Search HyP3 jobs since the reference scene's acquisition date and remove already submitted (in PENDING or RUNNING state) pairs.

    Args:
         pairs: A GeoDataFrame containing *at least*  these columns: `reference`, `reference_acquisition`, and
          `secondary`.

    Returns:
         The pairs GeoDataFrame with any already submitted pairs removed.
    """
    its_live_user = 'hyp3.its_live'

    pending_jobs = query_jobs_by_status_code(
        'PENDING', its_live_user, pairs.iloc[0].reference, pairs.iloc[0].reference_acquisition
    )
    running_jobs = query_jobs_by_status_code(
        'RUNNING', its_live_user, pairs.iloc[0].reference, pairs.iloc[0].reference_acquisition
    )
    jobs = pending_jobs + running_jobs

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
    submit: bool = True,
) -> sdk.Batch:
    """Trigger Landsat processing for a scene.

    Args:
        scene: Reference Landsat scene name to build pairs for.
        submit: Submit pairs to HyP3 for processing.

    Returns:
        Jobs submitted to HyP3 for processing.
    """
    pairs = None
    if scene.startswith('S2'):
        if is_new_scene(scene, log_level=logging.INFO):
            reference = get_sentinel2_stac_item(scene)
            if qualifies_for_sentinel2_processing(reference, log_level=logging.INFO):
                # hyp3-its-live will pull scenes from Google Cloud; ensure the new scene is there before processing
                # Note: Time between attempts is controlled by they SQS VisibilityTimeout
                raise_for_missing_in_google_cloud(scene)
                pairs = get_sentinel2_pairs_for_reference_scene(reference)

    else:
        reference = get_landsat_stac_item(scene)
        if qualifies_for_landsat_processing(reference, log_level=logging.INFO):
            pairs = get_landsat_pairs_for_reference_scene(reference)

    if pairs is None:
        return sdk.Batch()

    log.info(f'Found {len(pairs)} pairs for {scene}')
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
        log.debug(pairs.sort_values(by=['secondary'], ascending=False).loc[:, ['reference', 'secondary']])

    if len(pairs) > 0:
        pairs = deduplicate_hyp3_pairs(pairs)

        log.info(f'Deduplicated HyP3 running/pending pairs; {len(pairs)} remaining')
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
            log.debug(pairs.sort_values(by=['secondary'], ascending=False).loc[:, ['reference', 'secondary']])

        pairs = deduplicate_s3_pairs(pairs)

        log.info(f'Deduplicated already published pairs; {len(pairs)} remaining')
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
            _ = process_scene(message[product_id])
        except Exception:
            log.exception(f'Could not process message {record["messageId"]}')
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}


def main() -> None:
    """Command Line wrapper around `process_scene`."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('reference', help='Reference Landsat scene name to build pairs for')
    parser.add_argument('--submit', action='store_true', help='Submit pairs to HyP3 for processing')
    parser.add_argument('-v', '--verbose', action='store_true', help='Turn on verbose logging')
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    if args.verbose:
        log.setLevel(logging.DEBUG)

    log.debug(' '.join(sys.argv))
    _ = process_scene(args.reference, submit=args.submit)


if __name__ == '__main__':
    main()
