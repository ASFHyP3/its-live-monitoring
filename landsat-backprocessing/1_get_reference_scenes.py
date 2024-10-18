import json
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

import pystac_client


def process_tile(tile):
    client = pystac_client.Client.open('https://landsatlook.usgs.gov/stac-server')
    results = client.search(
        collections='landsat-c2l1',
        query=[
            f'landsat:wrs_path={tile[:3]}',
            f'landsat:wrs_row={tile[3:]}',
        ],
        datetime=[datetime(2022, 1, 1), datetime(2024, 8, 1)],
    )
    scenes = [item.id for page in results.pages() for item in page]
    return '\n'.join(scenes)


with open('../its_live_monitoring/src/landsat_tiles_to_process.json') as f:
    tiles = json.load(f)

with open(f'candidate_reference_scenes.txt', 'w') as f:
    with ProcessPoolExecutor() as executor:
        for results in executor.map(process_tile, tiles):
            if results:
                f.write(results + '\n')
