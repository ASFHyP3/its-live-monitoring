from collections import defaultdict
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

import sentinel2

# example scene name: S2A_MSIL1C_20200225T063841_N0209_R120_T44XNG_20200225T075814

INPUT_FILE = 'all_qualifying_scenes.txt'
OUTPUT_FILE = 'all_qualifying_pairs.csv'
MIN_SEPARATION_IN_SECONDS = sentinel2.SENTINEL2_MIN_PAIR_SEPARATION_IN_DAYS * 24 * 60 * 60
MAX_SEPARATION_IN_SECONDS = sentinel2.SENTINEL2_MAX_PAIR_SEPARATION_IN_DAYS * 24 * 60 * 60
MIN_REFERENCE_DATE = datetime(year=2022, month=1, day=1)
MAX_REFERENCE_DATE = datetime(year=2024, month=9, day=1)


def load_qualifying_scenes() -> list[str]:
    with open(INPUT_FILE) as f:
        return [line.strip() for line in f]


def group_scenes_by_orbit_and_tile(scenes: list[str]) -> list[list[str]]:
    stacks = defaultdict(list)
    for scene in scenes:
        _, _, _, _, absolute_orbit, tile, _ = scene.split('_')
        stacks[absolute_orbit + tile].append(scene)
    return list(stacks.values())


def get_acquisition_date(scene: str) -> datetime:
    return datetime.strptime(scene.split('_')[2], '%Y%m%dT%H%M%S')


def get_hash(scene: str) -> str:
    platform, product, acquisition_date, datatake, orbit, tile, processing_date = scene.split('_')
    return '_'.join([platform, product, acquisition_date, orbit, tile])


def remove_reprocessed_scenes(stack: list[str]) -> list[str]:
    previous_hash = ''
    new_stack = []
    for scene in stack:
        this_hash = get_hash(scene)
        if this_hash != previous_hash:
            new_stack.append(scene)
        previous_hash = this_hash
    return new_stack


def get_pairs_for_stack(stack: list[str]) -> str:
    pairs = ''
    stack.sort(reverse=True, key=get_acquisition_date)
    stack = remove_reprocessed_scenes(stack)
    for ii, reference in enumerate(stack):
        reference_date = get_acquisition_date(reference)
        if reference_date > MAX_REFERENCE_DATE:
            continue
        if reference_date < MIN_REFERENCE_DATE:
            break

        for secondary in stack[ii+1:]:
            separation_in_seconds = (reference_date - get_acquisition_date(secondary)).total_seconds()
            if separation_in_seconds < MIN_SEPARATION_IN_SECONDS:
                continue
            if separation_in_seconds > MAX_SEPARATION_IN_SECONDS:
                break
            pairs += f'{secondary},{reference}\n'
    return pairs


def main():
    scenes = load_qualifying_scenes()
    stacks = group_scenes_by_orbit_and_tile(scenes)
    with open(OUTPUT_FILE, 'w') as f:
        with ProcessPoolExecutor() as executor:
            for pair_list in executor.map(get_pairs_for_stack, stacks):
                f.write(pair_list)


if __name__ == '__main__':
    main()
