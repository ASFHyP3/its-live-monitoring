1. run `aws --no-sign-request s3 ls s3://its-live-data/velocity_image_pair/landsatOLI/v02/ --recursive | grep '.nc$' | cut -d '/' -f 5 | cut -d '_' -f 1-15 | sed 's/_X_/,/' > already_processed_pairs.csv` to build a csv of already-processed landsat scenes.
2. run `1_get_reference_scenes.py` to generate `candidate_reference_scenes.txt`, a list of landsat scenes over land ice 2022-01-01 to 2024-08-01 (~10 minutes)
3. run `2_build_pair_list.py` to generate `all_qualifying_pairs.csv`, a list of qualifying all qualifying pairs for all qualifying reference scenes 
4. run `3_deduplicate_pair_list.py` to generate `deduplicated_pairs.py`, the final list of pairs to process

5. TODO do some evaluations of output files
6. TODO build a script to submit these pairs to hyp3-its-live
