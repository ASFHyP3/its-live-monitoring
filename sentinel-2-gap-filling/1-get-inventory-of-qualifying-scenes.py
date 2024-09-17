"""Functions to support Sentinel-2 processing."""

import concurrent.futures
from datetime import datetime
import json

import sentinel2
from sentinel2 import SENTINEL2_TILES_TO_PROCESS as TILES

NUM_WORKERS = 8


def check_s2_pair_qualifies_for_processing(item) -> bool:
    try:
        # TODO: double-check if need to use other kwargs (relative_orbit, etc.)
        return sentinel2.qualifies_for_sentinel2_processing(item)
    except Exception as e:
        print(f'Unable to check {item.id} due to {e}')
        return False


def get_scene_names(tiles: list[str]) -> list[str]:
    results = sentinel2.SENTINEL2_CATALOG.search(
        collections=[sentinel2.SENTINEL2_COLLECTION_NAME],
        query={
            'eo:cloud_cover': {
                'lte': sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT,
            },
            'grid:code': {
                'in': [f'MGRS-{tile}' for tile in tiles],
            },
        },
        datetime=[datetime(2020, 7, 1), datetime(2024, 9, 1)],
    )
    return [
        item.id
        for page in results.pages()
        for item in page
        if sentinel2.qualifies_for_sentinel2_processing(item)
    ]


def main():
    scenes = []
    for tiles in (TILES[i:i+NUM_WORKERS] for i in range(0, len(TILES), NUM_WORKERS)):
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for scene in executor.map(get_scene_names, tiles):
                scenes.append(scene)

    with open('all_qualifying_s2_scenes.json', 'w') as f:
        json.dump(scenes, f)


if __name__ == "__main__":
    main()
