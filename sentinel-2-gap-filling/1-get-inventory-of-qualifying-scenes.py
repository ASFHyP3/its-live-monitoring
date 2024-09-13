"""Functions to support Sentinel-2 processing."""

import concurrent.futures
from datetime import datetime
import json

import sentinel2


def check_s2_pair_qualifies_for_processing(item):
    try:
        if sentinel2.qualifies_for_sentinel2_processing(item):
            return(item.id)
    except:
        print(f'Unable to check {item.id}')


def main():
    results = sentinel2.SENTINEL2_CATALOG.search(
        collections=[sentinel2.SENTINEL2_COLLECTION_NAME],
        query={
            'eo:cloud_cover': {
                'lte': sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT,
            },
            'grid:code': {
                'in': [f'MGRS-{tile}' for tile in sentinel2.SENTINEL2_TILES_TO_PROCESS],
            },
        },
        datetime=[datetime(2022, 2, 1), datetime(2023, 1, 1)],
    )

    items = []
    for page in results.pages():
        for result in page:
            items.append(result.to_dict())

    with open('all__s2_scenes.json', 'w') as f:
        json.dump(items, f)

    scenes = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for result in executor.map(check_s2_pair_qualifies_for_processing, items):
            if result:
                scenes.extend(result)

    with open('all_qualifying_s2_scenes.json', 'w') as f:
        json.dump(scenes, f)


if __name__ == "__main__":
    main()
