This folder contains one time scripts to inventory historical ITS_LIVE Sentinel-2 processing, identify unprocessed scenes/pairs, and submit those pairs to HyP3 for processing.

- 1-get-inventory-of-qualifying-scenes.py : queries the Google Cloud csv catalog to build a list Sentinel-2 L1C scenes from 2022-01-01 to present that qualify for processing.
  - Download the zipped file
  `wget https://storage.googleapis.com/gcp-public-data-sentinel-2/index.csv.gz`
    - A test sample set of these data are here: s3://jrs-dev/itslive/qualifying_s2_scenes.json
