"""Functions to support Sentinel-2 processing."""
from concurrent.futures import ProcessPoolExecutor
import csv
from datetime import datetime

import sentinel2
from pathlib import Path


def check_s2_pair_qualifies_for_processing(row, grid_tiles, search_date_window, date_format='%Y-%m-%dT%H:%M:%S.%fZ'):
    try:
        dtype = row['BASE_URL'].removesuffix('SAFE').split('_')[1]
        if dtype != 'MSIL1C':
            print(row['GRANULE_ID'])
        if row['MGRS_TILE'] not in grid_tiles:
            return False
        if float(row['CLOUD_COVER']) < sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT or \
                float(row['CLOUD_COVER']) is None:
            return False
        if datetime.strptime(row['SENSING_TIME'], date_format) < search_date_window[0] or \
                datetime.strptime(row['SENSING_TIME'], date_format) > search_date_window[1]:
            return False
        else:
            return row["GRANULE_ID"]
    except:
        print(f'Error processing: {row["GRANULE_ID"]}')
        return False


def main():
    s2_archive_file = Path('index.csv')

    search_date_window = [datetime(2020, 7, 1), datetime(2022, 1, 1)]
    grid_tiles = [tile for tile in sentinel2.SENTINEL2_TILES_TO_PROCESS]

    scenes = []
    with open(s2_archive_file, 'r') as f:
        reader = csv.DictReader(f)
        with ProcessPoolExecutor() as executor:
            for scene in executor.map(check_s2_pair_qualifies_for_processing, reader):
                if scene:
                    scenes.append(scene)

    with open('all_qualifying_s2_scenes.txt', 'w') as f:
        for scene in scenes:
            f.write(scene + '\n')


if __name__ == "__main__":
    main()
