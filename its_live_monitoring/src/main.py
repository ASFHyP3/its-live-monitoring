"""Lambda function to trigger low-latency Landsat and Sentinel-2 processing from newly acquired scenes."""

import argparse
import json
import logging
import os
import sys

import hyp3_sdk as sdk
import pandas as pd

from hyp3 import deduplicate_hyp3_pairs, submit_pairs_for_processing
from itslive import deduplicate_published_pairs
from landsat import (
    get_landsat_pairs_for_reference_scene,
    get_landsat_stac_item,
    qualifies_for_landsat_processing,
)
from nisar import (
    get_nisar_cmr_item,
    get_nisar_pairs_for_reference_scene,
    product_qualifies_for_nisar_processing,
)
from sentinel1 import (
    get_sentinel1_cmr_item,
    get_sentinel1_pairs_for_reference_scene,
    product_qualifies_for_sentinel1_processing,
)
from sentinel2 import (
    get_sentinel2_pairs_for_reference_scene,
    get_sentinel2_stac_item,
    is_new_scene,
    qualifies_for_sentinel2_processing,
    raise_for_missing_in_google_cloud,
)


log = logging.getLogger('its_live_monitoring')
log.setLevel(os.environ.get('LOGGING_LEVEL', 'INFO'))


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
    elif scene.startswith('L'):
        reference = get_landsat_stac_item(scene)
        if qualifies_for_landsat_processing(reference, log_level=logging.INFO):
            pairs = get_landsat_pairs_for_reference_scene(reference)
    elif scene.startswith('S1'):
        reference = get_sentinel1_cmr_item(scene)
        if product_qualifies_for_sentinel1_processing(reference, log_level=logging.INFO):
            pairs = get_sentinel1_pairs_for_reference_scene(reference)
    elif scene.startswith('NISAR'):
        reference = get_nisar_cmr_item(scene)
        if product_qualifies_for_nisar_processing(reference, log_level=logging.INFO):
            pairs = get_nisar_pairs_for_reference_scene(reference)

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

    if len(pairs) > 0:
        pairs = deduplicate_published_pairs(pairs)

        log.info(f'Deduplicated published ITS_LIVE pairs; {len(pairs)} remaining')
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
            log.debug(pairs.sort_values(by=['secondary'], ascending=False).loc[:, ['reference', 'secondary']])

    jobs = sdk.Batch()
    if submit:
        jobs += submit_pairs_for_processing(pairs)

    log.info(jobs)
    return jobs


def product_id_from_message(message: dict) -> str:
    """Return a scene product ID from an SQS message.

    Args:
        message: SQS message as received from supported satellite missions (Landsat, Sentinel-1, and Sentinel-2)

    Returns:
        product_id: the product ID of a scene
    """
    # See `tests/integration/*-valid.json` for example messages
    match message:
        case {'landsat_product_id': product_id} if product_id.startswith('L'):
            return product_id
        case {'name': product_id} if product_id.startswith('S2'):
            return product_id
        case {'granule_ur': product_id} if product_id.startswith('S1'):
            return product_id
        case {'granule_ur': product_id} if product_id.startswith('NISAR'):
            return product_id
        case _:
            raise ValueError(f'Unable to determine product ID from message {message}')


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
            product_id = product_id_from_message(message)
            _ = process_scene(product_id)
        except Exception:
            log.exception(f'Could not process message {record["messageId"]}')
            batch_item_failures.append({'itemIdentifier': record['messageId']})
    return {'batchItemFailures': batch_item_failures}


def main() -> None:
    """Command Line wrapper around `process_scene`."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('reference', help='Reference scene name to build pairs for')
    parser.add_argument('--submit', action='store_true', help='Submit pairs to HyP3 for processing')
    parser.add_argument(
        '-v', '--verbose', action='count', default=0, help='Increase the logging verbosity. Can be used multiple times.'
    )
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
    if args.verbose > 0:
        log.setLevel(logging.DEBUG)
    if args.verbose < 2:
        import asf_search

        asf_logger = logging.getLogger(asf_search.__name__)
        asf_logger.disabled = True

    log.debug(' '.join(sys.argv))
    _ = process_scene(args.reference, submit=args.submit)


if __name__ == '__main__':
    main()
