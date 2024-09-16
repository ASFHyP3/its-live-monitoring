"""Functions to support Sentinel-2 processing."""

import concurrent.futures
import csv
from datetime import datetime
import json

import sentinel2
from pathlib import Path


def check_s2_pair_qualifies_for_processing(row, grid_tiles, search_date_window, date_format='%Y-%m-%dT%H:%M:%S.%fZ'):
    dtype = row['BASE_URL'].removesuffix('SAFE').split('_')[1]
    if float(row['CLOUD_COVER']) < sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT:
        print(f'CLOUD COVER {row["GRANULE_ID"]}')
        return False
    if datetime.strptime(row['SENSING_TIME'], date_format) < search_date_window[0] or \
            datetime.strptime(row['SENSING_TIME'], date_format) > search_date_window[1]:
        print(f'dATE {row["GRANULE_ID"]}')
        return False
    if row['MGRS_TILE'] not in grid_tiles:
        print(f'MGRS {row["GRANULE_ID"]}')
        return False
    if dtype != 'MSIL1C':
        print(row['GRANULE_ID'])
    else:
        return True


def main():
    s2_archive_file = Path('index.csv')

    search_date_window = [datetime(2022, 2, 1), datetime(2023, 1, 1)]
    grid_tiles = [tile for tile in sentinel2.SENTINEL2_TILES_TO_PROCESS]

    scenes = []
    with open(s2_archive_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if check_s2_pair_qualifies_for_processing(row, grid_tiles, search_date_window):
                scenes.extend(row)

    with open('all_qualifying_s2_scenes.json', 'w') as f:
        json.dump(scenes, f)


if __name__ == "__main__":
    main()
