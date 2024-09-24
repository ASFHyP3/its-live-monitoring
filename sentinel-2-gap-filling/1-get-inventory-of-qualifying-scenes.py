"""Functions to support Sentinel-2 processing."""

import concurrent.futures
import json
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

import pystac

import sentinel2
from sentinel2 import SENTINEL2_TILES_TO_PROCESS as TILES


OUTPUT_DIR = Path('qualifying-scenes')

NUM_CPU = 32

START = datetime(2020, 7, 1)
STOP = datetime(2024, 9, 1)
INTERVAL = timedelta(days=30)


def get_timeframes() -> list[list[datetime]]:
    start = START
    timeframes = []
    while start != STOP:
        stop = min(start + INTERVAL, STOP)
        timeframes.append([start, stop])
        start = stop
    return timeframes


TIMEFRAMES = get_timeframes()


def exception_to_str(e: Exception) -> str:
    return f'{type(e).__name__}: {e}'


def exists_in_google_cloud(scene_name: str) -> bool:
    try:
        sentinel2.raise_for_missing_in_google_cloud(scene_name)
        return True
    except Exception as e:
        print(f'Scene {scene_name} not found in Google Cloud due to {exception_to_str(e)}')
        return False


def check_s2_pair_qualifies_for_processing(item: pystac.Item) -> bool:
    try:
        return sentinel2.qualifies_for_sentinel2_processing(item)
    except Exception as e:
        print(f'Unable to check {item.id} due to {exception_to_str(e)}')
        return False


def query_scene_names(worker_id: str, tiles: list[str], timeframe: list[datetime]) -> list[str]:
    while True:
        try:
            results = sentinel2.SENTINEL2_CATALOG.search(
                collections=[sentinel2.SENTINEL2_COLLECTION_NAME],
                query={
                    'eo:cloud_cover': {
                        'lte': sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT,
                    },
                    'grid:code': {
                        'in': tiles
                    },
                },
                datetime=timeframe,
            )
            scene_names = [
                item.properties['s2:product_uri'].removesuffix('.SAFE')
                for page in results.pages()
                for item in page
                if check_s2_pair_qualifies_for_processing(item)
                and exists_in_google_cloud(item.properties['s2:product_uri'].removesuffix('.SAFE'))
            ]
            return scene_names
        except Exception as e:
            print(f'Worker {worker_id}: STAC search failed due to {exception_to_str(e)}')
            time.sleep(random.randint(10, 50))


def get_scene_names_for_timeframe(worker_id: str, tiles: list[str], timeframe: list[datetime]) -> list[str]:
    filename = OUTPUT_DIR / f'{worker_id}-{timeframe[0].isoformat()}'
    if filename.exists():
        with filename.open() as f:
            scene_names = json.load(f)
        print(f'Worker {worker_id}: Loaded {len(scene_names)} scene names from disk for timeframe {timeframe}')
    else:
        print(f'Worker {worker_id}: Getting scene names for timeframe {timeframe}')
        scene_names = query_scene_names(worker_id, tiles, timeframe)
        print(f'Worker {worker_id}: Got {len(scene_names)} scene names for timeframe {timeframe}')
        with filename.open('w') as f:
            json.dump(scene_names, f)
    return scene_names


def get_scene_names(tile_chunk: tuple[str, list[str]]) -> list[str]:
    worker_id, tiles = tile_chunk
    print(f'Worker {worker_id}: Processing {len(tiles)} tiles')
    scene_names = [
        scene_name
        for timeframe in TIMEFRAMES
        for scene_name in get_scene_names_for_timeframe(worker_id, tiles, timeframe)
    ]
    print(f'Worker {worker_id}: Finished with {len(scene_names)} scene names')
    return scene_names


def main() -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)

    print('Timeframes:')
    for timeframe in TIMEFRAMES:
        print(timeframe[0].isoformat(), timeframe[1].isoformat())

    chunksize = len(TILES) // NUM_CPU
    print(f'\nChunk size: {chunksize}\n')

    tile_chunks = [
        (str(count).zfill(2), TILES[i : i + chunksize])
        for count, i in enumerate(range(0, len(TILES), chunksize), start=1)
    ]

    with concurrent.futures.ProcessPoolExecutor() as executor:
        scenes = [
            scene
            for batch_of_scenes in executor.map(get_scene_names, tile_chunks)
            for scene in batch_of_scenes
        ]
    print(f'\nGot {len(scenes)} total scenes')

    with (OUTPUT_DIR / 'all_qualifying_s2_scenes.json').open('w') as f:
        json.dump(scenes, f)


if __name__ == '__main__':
    main()
