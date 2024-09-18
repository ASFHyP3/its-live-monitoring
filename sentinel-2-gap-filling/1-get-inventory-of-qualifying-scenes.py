"""Functions to support Sentinel-2 processing."""

import concurrent.futures
from datetime import datetime, timedelta
import json

import requests

import sentinel2
from sentinel2 import SENTINEL2_TILES_TO_PROCESS as TILES

NUM_WORKERS = 32

START = datetime(2020, 7, 1)
STOP = datetime(2024, 9, 1)
INTERVAL = timedelta(days=180)

def get_timeframes():
    start = START
    timeframes = []
    while start != STOP:
        stop = min(start + INTERVAL - timedelta(seconds=1), STOP)
        timeframes.append([start, stop])
        start = stop
    return timeframes


TIMEFRAMES = get_timeframes()


def exists_in_google_cloud(scene_name: str) -> bool:
    try:
        sentinel2.raise_for_missing_in_google_cloud(scene_name)
        return True
    except requests.HTTPError as e:
        print(f'Scene {scene_name} not found in Google Cloud due to {e}')
        return False


def check_s2_pair_qualifies_for_processing(item) -> bool:
    try:
        return sentinel2.qualifies_for_sentinel2_processing(item)
    except Exception as e:
        print(f'Unable to check {item.id} due to {e}')
        return False


def get_scene_names_for_timeframe(tiles: list[str], timeframe: list[datetime]) -> list[str]:
    print(f'Getting scene names for {len(tiles)} tiles and timeframe {timeframe}')
    while True:
        try:
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
                datetime=timeframe,
            )
        except Exception as e:
            print(f'STAC search failed due to {e}')
            continue
    return [
        item.id
        for page in results.pages()
        for item in page
        if check_s2_pair_qualifies_for_processing(item)
           and exists_in_google_cloud(item.properties['s2:product_uri'].removesuffix('.SAFE'))
    ]


def get_scene_names(tiles: list[str]) -> list[str]:
    return [
        scene_name
        for timeframe in TIMEFRAMES
        for scene_name in get_scene_names_for_timeframe(tiles, timeframe)
    ]


def main():
    print('Timeframes:')
    for timeframe in TIMEFRAMES:
        print(timeframe[0].isoformat(), timeframe[1].isoformat())

    chunksize = len(TILES) // NUM_WORKERS
    print(f'\nChunk size: {chunksize}\n')

    tile_chunks = [TILES[i:i + chunksize] for i in range(0, len(TILES), chunksize)]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        scenes = [
            scene
            for batch_of_scenes in executor.map(get_scene_names, tile_chunks)
            for scene in batch_of_scenes
        ]
    print(f'\nGot {len(scenes)} total scenes')

    with open('all_qualifying_s2_scenes.json', 'w') as f:
        json.dump(scenes, f)


if __name__ == "__main__":
    main()
