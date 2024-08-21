import argparse
import os
from collections import Counter


def write_secondary_count_frequencies(rows: list[tuple], dirname: str) -> None:
    reference_scenes = (row[1] for row in rows)

    # The number of times each reference appears in the csv is the number of secondary scenes for that reference.
    secondary_counts = Counter(reference_scenes)
    secondary_count_frequencies = Counter(secondary_counts.values())

    # number of unique reference scenes
    assert len(secondary_counts.keys()) == sum(secondary_count_frequencies.values())

    output_rows = sorted(secondary_count_frequencies.items())
    # for row in output_rows:
    #     print(row[0], '\t', '#' * row[1])

    write_csv(
        ('secondary_count', 'frequency'),
        output_rows,
        'secondary-count-frequencies.csv',
        dirname,
    )


def write_tile_counts(rows: list[tuple], dirname: str) -> None:
    secondary_tile_counts = Counter(row[0].split('_')[2] for row in rows)
    reference_tile_counts = Counter(row[1].split('_')[2] for row in rows)

    assert secondary_tile_counts == reference_tile_counts

    output_rows = sorted(secondary_tile_counts.items())
    write_csv(
        ('tile', 'count'),
        output_rows,
        'tile-counts.csv',
        dirname,
    )


def write_month_counts(rows: list[tuple], dirname: str) -> None:
    reference_acquisition_month_counts = Counter(row[1].split('_')[3][0:6] for row in rows)
    output_rows = sorted(reference_acquisition_month_counts.items())
    write_csv(
        ('reference_acquisition_month', 'count'),
        output_rows,
        'reference-acquisition-month-counts.csv',
        dirname,
    )



def write_csv(first_row: tuple, rows: list[tuple], csvname: str, dirname: str) -> None:
    with open(os.path.join(dirname, csvname), 'w') as f:
        f.write(','.join(first_row) + '\n')
        for row in rows:
            f.write(','.join(map(str, row)) + '\n')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('pairs_csv')
    return parser.parse_args()


def main():
    args = parse_args()
    dirname = str(os.path.basename(args.pairs_csv).removesuffix('.csv')) + '_histograms'
    os.mkdir(dirname)

    with open(args.pairs_csv) as f:
        lines = f.read().strip('\n').split('\n')

    rows = [tuple(line.split(',')) for line in lines]

    write_secondary_count_frequencies(rows, dirname)
    write_tile_counts(rows, dirname)
    write_month_counts(rows, dirname)


if __name__ == '__main__':
    main()
