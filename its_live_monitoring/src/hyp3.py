"""Functions for interacting with HyP3 ITS_LIVE."""

import logging
import os
from copy import deepcopy
from datetime import UTC, datetime

import boto3
import geopandas as gpd
import hyp3_sdk as sdk
import pandas as pd
from boto3.dynamodb.conditions import Attr, Key


log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))

dynamo = boto3.resource('dynamodb')

# NOTE: Commented items will get set when submitting
AUTORIFT_JOB_TEMPLATE = {
    'job_parameters': {
        # 'reference': list[str],
        # 'secondary': list[str],
        'parameter_file': '/vsicurl/https://its-live-data.s3.amazonaws.com/autorift_parameters/v001/autorift_landice_0120m.shp',
        # 'publish_bucket': str | None,
        'use_static_files': True,
        # 'frame_id' = str | None,
        # 'stac_items_endpoint': str | None,
        # 'stac_exists_ok': bool,
    },
    'job_type': 'AUTORIFT',
    # 'name': str | None,
}


def format_time(time: datetime) -> str:
    """Format time to ISO with UTC timezone.

    Args:
        time: a datetime object to format

    Returns:
        datetime: the UTC time in ISO format
    """
    if time.tzinfo is None:
        raise ValueError(f'missing tzinfo for datetime {time}')
    utc_time = time.astimezone(UTC)
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


def get_reference_secondary_from_job(job: sdk.Job) -> tuple[tuple | str, tuple | str]:
    """Get the reference and secondary scenes from an AUTORIFT HyP3 job."""
    granules = job.job_parameters.get('granules')
    if granules:
        reference = granules[:1]
        secondary = granules[1:]
    else:
        reference = job.job_parameters['reference']
        secondary = job.job_parameters['secondary']

    return tuple(reference), tuple(secondary)


def deduplicate_hyp3_pairs(pairs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Search HyP3 jobs since the reference scene's acquisition date and remove already submitted (in PENDING or RUNNING state) pairs.

    Args:
         pairs: A GeoDataFrame containing *at least*  these columns: `reference`, `reference_acquisition`, and
          `secondary`.

    Returns:
         The pairs GeoDataFrame with any already submitted pairs removed.
    """
    earthdata_username = os.environ['EARTHDATA_USERNAME']
    assert earthdata_username is not None

    pending_jobs = query_jobs_by_status_code(
        status_code='PENDING',
        user=earthdata_username,
        name=pairs.iloc[0].job_name,
        start=pairs.iloc[0].reference_acquisition,
    )
    running_jobs = query_jobs_by_status_code(
        status_code='RUNNING',
        user=earthdata_username,
        name=pairs.iloc[0].job_name,
        start=pairs.iloc[0].reference_acquisition,
    )
    jobs = pending_jobs + running_jobs

    df = pd.DataFrame([get_reference_secondary_from_job(job) for job in jobs], columns=['reference', 'secondary'])
    df = df.set_index(['reference', 'secondary'])
    pairs = pairs.set_index(['reference', 'secondary'])

    duplicates = df.loc[df.index.isin(pairs.index)]
    if len(duplicates) > 0:
        pairs = pairs.drop(duplicates.index)

    return pairs.reset_index()


def _nullable_str(s: str) -> str | None:
    s = s.replace('None', '').strip()
    return s if s else None


def _string_is_true(s: str) -> bool:
    return s.lower() == 'true'


def submit_pairs_for_processing(pairs: gpd.GeoDataFrame) -> sdk.Batch:  # noqa: D103
    prepared_jobs = []
    for reference, secondary, name in pairs[['reference', 'secondary', 'job_name']].itertuples(index=False):
        prepared_job: dict = deepcopy(AUTORIFT_JOB_TEMPLATE)
        prepared_job['name'] = name
        prepared_job['job_parameters']['reference'] = reference
        prepared_job['job_parameters']['secondary'] = secondary

        if publish_bucket := os.environ.get('PUBLISH_BUCKET', ''):
            prepared_job['job_parameters']['publish_bucket'] = _nullable_str(publish_bucket)

        if stac_items_endpoints := os.environ.get('STAC_ITEMS_ENDPOINT', ''):
            prepared_job['job_parameters']['stac_items_endpoint'] = _nullable_str(stac_items_endpoints)
            prepared_job['job_parameters']['stac_exists_ok'] = _string_is_true(os.environ.get('STAC_EXISTS_OK', ''))

        if name.startswith('OPERA'):
            prepared_job['job_parameters']['frame_id'] = name.split('_')[1]

        prepared_jobs.append(prepared_job)

    log.debug(prepared_jobs)

    hyp3 = sdk.HyP3(
        os.environ.get('HYP3_API', 'https://hyp3-its-live-test.asf.alaska.edu'),
        username=os.environ.get('EARTHDATA_USERNAME'),
        password=os.environ.get('EARTHDATA_PASSWORD'),
    )

    jobs = sdk.Batch()
    for batch in sdk.util.chunk(prepared_jobs):
        jobs += hyp3.submit_prepared_jobs(batch)

    return jobs
