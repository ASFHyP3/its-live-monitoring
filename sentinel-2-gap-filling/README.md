This folder contains one time scripts to inventory historical ITS_LIVE Sentinel-2 processing, identify unprocessed scenes/pairs, and submit those pairs to HyP3 for processing.

- 1-get-inventory-of-qualifying-scenes.py : queries a local copy of the Google Cloud  to build a list Sentinel-2 L1C scenes from 2022-01-01 to present that qualify for processing.

- 2-build-pair-list-from-qualifying-scenes.py: builds a csv of `secondary_scene,reference_scene` pairs from a list of qualifying scenes

- run `aws --no-sign-request s3 ls s3://its-live-data/velocity_image_pair/sentinel2/v02/ --recursive | grep '.nc$' > sentinel2_s3_inventory.txt` to generate a list of already-processed data files.
  - output is at `s3://asj-dev/its-live/sentinel-2/sentinel2_s3_inventory.txt.zip` (299 MB)
- run `cut -d '/' -f 5 sentinel2_s3_inventory.txt | cut -d '_' -f 1-15 | sed 's/_X_/,/' | sed 's/.nc$//' > already_processed_pairs.csv` to reduce the inventory to a csv of already-processed `secondary,referece` scene names
  - output is at `s3://asj-dev/its-live/sentinel-2/already_processed_pairs.csv.zip` (154 MB)
