"""Lambda function to trigger low-latency Landsat processing from newly acquired scenes."""

import json
import os
from datetime import timedelta
from pathlib import Path

import hyp3_sdk as sdk
import pystac
import pystac_client


LANDSAT_STAC_API = 'https://landsatlook.usgs.gov/stac-server'
LANDSAT_CATALOG = pystac_client.Client.open(LANDSAT_STAC_API)
LANDSAT_COLLECTION = 'landsat-c2l1'
LANDSAT_TILES_TO_PROCESS = json.loads((Path(__file__).parent / 'landsat_tiles_to_process.json').read_text())

MAX_PAIR_SEPARATION_IN_DAYS = 544

EARTHDATA_USERNAME = os.environ.get('EARTHDATA_USERNAME')
EARTHDATA_PASSWORD = os.environ.get('EARTHDATA_PASSWORD')
HYP3 = sdk.HyP3(
    os.environ.get('HYP3_API', 'https://hyp3-its-live.asf.alaska.edu'),
    username=EARTHDATA_USERNAME,
    password=EARTHDATA_PASSWORD,
)


def _qualifies_for_processing(item: pystac.item.Item) -> bool:
    return (
        item.collection_id == 'landsat-c2l1'
        and 'OLI' in item.properties['instruments']
        and item.properties['landsat:collection_category'] in ['T1', 'T2']
        and item.properties['landsat:wrs_path'] + item.properties['landsat:wrs_row'] in LANDSAT_TILES_TO_PROCESS
        and item.properties['eo:cloud_cover'] < 60
        and item.properties['view:off_nadir'] == 0
    )


def _get_stac_item(scene: str) -> pystac.item.Item:
    collection = LANDSAT_CATALOG.get_collection(LANDSAT_COLLECTION)
    item = collection.get_item(scene)
    if item is None:
        raise ValueError(f'Scene {scene} not found in STAC catalog')
    return item


def get_landsat_secondaries_for_reference_scene(
    reference: pystac.item.Item,
    max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPARATION_IN_DAYS),
) -> list[pystac.item.Item]:
    """Generate potential ITS_LIVE velocity pairs for a given Landsat scene.

    Args:
        reference: STAC item of the Landsat reference scene to find pairs for
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary scenes

    Returns:
        A list of STAC items for all potential secondary scenes to pair with a landsat reference scene.
    """
    results = LANDSAT_CATALOG.search(
        collections=[reference.collection_id],
        query=[
            f'landsat:wrs_path={reference.properties["landsat:wrs_path"]}',
            f'landsat:wrs_row={reference.properties["landsat:wrs_row"]}',
        ],
        datetime=[reference.datetime - max_pair_separation, reference.datetime - timedelta(seconds=1)],
    )
    items = [item for page in results.pages() for item in page if _qualifies_for_processing(item)]
    return items


def deduplicate_hyp3_pairs(reference: pystac.item.Item, secondaries: list[pystac.item.Item]) -> list[pystac.item.Item]:
    """Ensure we don't submit duplicate jobs to HyP3.

    Search HyP3 jobs since the reference scene's acquisition date and remove already processed pairs

    Args:
         reference: A STAC item for the reference scene
         secondaries: A list of STAC items for the secondary scenes

    Returns:
         The list of secondaries with any that have already been submitted removed.
    """
    jobs = HYP3.find_jobs(
        job_type='AUTORIFT',
        start=reference.datetime,
        name=reference.id,
        user_id=EARTHDATA_USERNAME,
    )

    already_processed = [job.job_parameters['granules'][1] for job in jobs]
    deduplicated_secondaries = [item for item in secondaries if item.id not in already_processed]
    return deduplicated_secondaries


def submit_pairs_for_processing(reference: pystac.item.Item, secondaries: list[pystac.item.Item]) -> sdk.Batch:
    prepared_jobs = []
    for secondary in secondaries:
        prepared_jobs.append(HYP3.prepare_autorift_job(reference.id, secondary.id, name=reference.id))

    jobs = sdk.Batch()
    for batch in sdk.util.chunk(prepared_jobs):
        jobs += HYP3.submit_prepared_jobs(batch)

    return jobs


def process_scene(
    scene: str,
    max_pair_separation: timedelta = timedelta(days=MAX_PAIR_SEPARATION_IN_DAYS),
    submit: bool = True,
) -> sdk.Batch:
    """Trigger Landsat processing for a scene.

    Args:
        scene: Reference Landsat scene name to build pairs for.
        max_pair_separation: How many days back from a reference scene's acquisition date to search for secondary
         scenes.
        submit: Submit pairs to HyP3 for processing.

    Returns:
        Jobs submitted to HyP3 for processing.
    """
    reference = _get_stac_item(scene)

    if not _qualifies_for_processing(reference):
        print(f'Reference scene {scene} does not qualify for processing')
        return sdk.Batch()

    secondaries = get_landsat_secondaries_for_reference_scene(reference, max_pair_separation)
    print(f'Found {len(secondaries)} pairs for {scene}')

    secondaries = deduplicate_hyp3_pairs(reference, secondaries)
    print(f'Deduplicated pairs; {len(secondaries)} remaining')

    jobs = sdk.Batch()
    if submit:
        jobs += submit_pairs_for_processing(reference, secondaries)

    print(jobs)


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
            process_scene(message['landsat_product_id'])
        except Exception as e:
            print(f'Could not process message {record["messageId"]}')
            print(e)
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}
