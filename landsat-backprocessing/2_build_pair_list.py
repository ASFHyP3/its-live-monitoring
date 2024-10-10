from concurrent.futures import ProcessPoolExecutor

import landsat


def get_pairs_list_for_scene(reference: str) -> str:
    pair_list = ''
    try:
        item = landsat.get_landsat_stac_item(reference)
        if landsat.qualifies_for_landsat_processing(item):
            for secondary in landsat.get_landsat_pairs_for_reference_scene(item):
                pair_list += f'{secondary},{reference}\n'
    except Exception as e:
        print(f'{reference}: {e}')
    return pair_list


with open ('candidate_reference_scenes.txt') as f:
    reference_scenes = [line.strip() for line in f]

with open('all_qualifying_pairs.csv', 'w') as f:
    with ProcessPoolExecutor(max_workers=30) as executor:
        for pair_list in executor.map(get_pairs_list_for_scene, reference_scenes):
            if pair_list:
                f.write(pair_list)
