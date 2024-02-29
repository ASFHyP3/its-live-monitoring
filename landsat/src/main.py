import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import geopandas as gpd
import hyp3_sdk as sdk
import pandas as pd
import pystac_client
from dateutil.parser import parse as date_parser


# FIXME: specify earthdata username and password?
# FIXME: don't hardcode HyP3 API here
HYP3 = sdk.HyP3('https://hyp3-its-live.asf.alaska.edu')

STAC_CLIENT = pystac_client.Client
LANDSAT_STAC_API = 'https://landsatlook.usgs.gov/stac-server'
LANDSAT_CATALOG = pystac_client.Client.open(LANDSAT_STAC_API)
LANDSAT_COLLECTION = 'landsat-c2l1'

MAX_PAIR_SEPERATION = 544  # days
MAX_CLOUD_COVER_PERCENT = 60

log = logging.getLogger(__name__)


def search_date(date_string: str) -> datetime:
    default_date = datetime(2020, 1, 1, 0, 0, 0, 0, timezone.utc)
    dt = date_parser(date_string, default=default_date)
    return dt.astimezone(tz=timezone.utc)


def check_scene(scene, max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT):
    collection = LANDSAT_CATALOG.get_collection(LANDSAT_COLLECTION)
    item = collection.get_item(scene)
    # TODO: raise specific errors instead of asserts
    assert item.properties['eo:cloud_cover'] < max_cloud_cover
    assert item.properties['view:off_nadir'] == 0


def get_landsat_pairs_for_reference_scene(
    reference: str,
    max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPERATION),
    max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT,
) -> gpd.GeoDataFrame:
    """Generate potential ITS_LIVE velocity pairs for a given Landsat scene

    Args:
        reference: Landsat reference scene to find pairs for
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary scenes
        max_cloud_cover: The maximum percent of the secondary scene that can be covered by clouds

    Returns:
        A DataFrame with all potential pairs for a landsat reference scene. Metadata in the columns will be for the
        *secondary* scene unless specified otherwise.
    """
    path = reference.split('_')[2][0:3]
    row = reference.split('_')[2][3:]
    acquisition_time = search_date(reference.split('_')[3])

    results = LANDSAT_CATALOG.search(
        collections=[LANDSAT_COLLECTION],
        query=[
            f'landsat:wrs_path={path}',
            f'landsat:wrs_row={row}',
            f'eo:cloud_cover<{max_cloud_cover}',
            # TODO: off-nadir handling
            'view:off_nadir=0',
        ],
        datetime=[acquisition_time - max_pair_separation, acquisition_time],
    )
    items = [ii for iis in results.pages() for ii in iis]

    features = []
    for item in items:
        feature = item.to_dict()
        feature['properties']['id'] = item.id
        features.append(feature)

    df = gpd.GeoDataFrame.from_features(features)
    df['datetime'] = pd.to_datetime(df.datetime)

    # FIXME: This duplicates the search parameters... do we still want these filters?
    sec_scenes = df[
        (acquisition_time.date() > df.datetime.dt.date)
        & (df.datetime.dt.date > (acquisition_time.date() - max_pair_separation))
    ]

    sec_scenes = sec_scenes.rename(columns={'id': 'secondary'})
    sec_scenes['reference'] = reference
    sec_scenes['reference_acquisition'] = acquisition_time

    return sec_scenes


# NOTE: Since each newly ingested Landsat scene will become a new unique reference scene, we don't need to look in the
#       catalog to deduplicate. We do need to make sure we haven't already requested HyP3 jobs, but we only need to look
#       back as far as the search start time as HyP3 jobs *must* have been submitted after the reference scene was
#       acquired.
#
#       When *adding* tiles to the pair list, new scenes will get picked up automatically, but we'll need to manually
#       generate pairs all the way back in time. Marks scripts generate all possible pairs for all time.
def deduplicate_hyp3_pairs(pairs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # FIXME: do we want to hardcode job type and username here?
    jobs = HYP3.find_jobs(start=pairs.iloc[0].reference_acquisition, job_type='AUTORIFT', user_id='hyp3.its_live')

    df = pd.DataFrame([job.job_parameters['granules'] for job in jobs], columns=['reference', 'secondary'])

    df = df.set_index(['reference', 'secondary'])
    pairs = pairs.set_index(['reference', 'secondary'])

    duplicates = df.loc[df.index.isin(pairs.index)]
    if len(duplicates) > 0:
        pairs = pairs.drop(duplicates.index)

    return pairs.reset_index()


def submit_pairs_for_processing(pairs: gpd.GeoDataFrame) -> sdk.Batch:
    prepared_jobs = []
    for reference, secondary in pairs[['reference', 'secondary']].itertuples(index=False):
        tile = reference.split('_')[2]
        prepared_jobs.append(HYP3.prepare_autorift_job(reference, secondary, name=tile))

    jobs = sdk.Batch()
    for batch in sdk.util.chunk(prepared_jobs):
        jobs += HYP3.submit_prepared_jobs(batch)

    return jobs


def process_scene(
    scene,
    max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPERATION),
    max_cloud_cover: int = MAX_CLOUD_COVER_PERCENT,
    submit: bool = True,
) -> sdk.Batch:
    # TODO: error handling
    check_scene(scene, max_cloud_cover)

    pairs = get_landsat_pairs_for_reference_scene(scene, max_pair_separation, max_cloud_cover)
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

    logging.info(jobs)
    return jobs


def lambda_handler(event: dict, context: Any):
    """Landsat processing lambda function.

    Accepts an event with SQS records for newly ingested Landsat scenes and processes each scene.

    Args:
        event: The event dictionary that contains the parameters sent when this function is invoked.
        context: The context in which is function is called.
    """
    for record in event['Records']:
        body = json.loads(record['body'])
        message = json.loads(body['Message'])
        _ = process_scene(message['landsat_product_id'])


# FIXME
def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('reference', help='Reference Landsat scene name to build pairs for')
    parser.add_argument(
        '--max-pair-separation',
        type=int,
        default=MAX_PAIR_SEPERATION,
        help="How many days back from a reference scene's acquisition date " 'to search for secondary scenes',
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
