"""Functions to support Sentinel-2 processing."""
import csv
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from pathlib import Path

import requests

import pandas as pd

SESSION = requests.Session()
HEADER = 'https://roda.sentinel-hub.com/sentinel-s2-l1c/tiles/'


def get_takes_for_scene(row):
    print(row)
    url_MGRS = row[1]['MGRS_TILE'][0:2] + '/' + row[1]['MGRS_TILE'][2:3] + '/' + row[1]['MGRS_TILE'][3:5]
    url_date = row[1]['SENSING_TIME'][0:4] + '/' + row[1]['SENSING_TIME'][5:7] + '/' + row[1]['SENSING_TIME'][8:10] + '/'

    pages_url = HEADER + url_MGRS + url_date
    response = SESSION.get(pages_url)
    response.raise_for_status()
    if response.json()['CommonPrefixes']:
        print(response.json()['KeyCount'])
        return response.json()['KeyCount'] - 1
    else:
        return 0


def main():
    s2_archive_file = Path('subindex.csv')
    csv_input = pd.read_csv(s2_archive_file)

    csv_input['NUM_TAKES'] = [get_takes_for_scene(row) for row in csv_input.iterrows()]
    csv_input.to_csv('appended_index.csv')


if __name__ == "__main__":
    main()
