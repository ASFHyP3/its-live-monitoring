from collections import Counter


def write_secondary_count_frequencies(rows: list[tuple]) -> None:
    reference_scenes = (row[1] for row in rows)

    # The number of times each reference appears in the csv is the number of secondary scenes for that reference.
    secondary_counts = Counter(reference_scenes)
    secondary_count_frequencies = Counter(secondary_counts.values())

    # number of unique reference scenes
    assert len(secondary_counts.keys()) == sum(secondary_count_frequencies.values()) == 59

    output_rows = sorted(secondary_count_frequencies.items())
    # for row in output_rows:
    #     print(row[0], '\t', '#' * row[1])

    write_csv(('secondary_count', 'frequency'), output_rows, 'secondary-count-frequencies.csv')


def write_tile_counts(rows: list[tuple]) -> None:
    secondary_tile_counts = Counter(row[0].split('_')[2] for row in rows)
    reference_tile_counts = Counter(row[1].split('_')[2] for row in rows)

    assert secondary_tile_counts == reference_tile_counts

    output_rows = sorted(secondary_tile_counts.items())
    write_csv(('tile', 'count'), output_rows, 'tile-counts.csv')


def write_month_counts(rows: list[tuple]) -> None:
    reference_acquisition_month_counts = Counter(row[1].split('_')[3][0:6] for row in rows)
    output_rows = sorted(reference_acquisition_month_counts.items())
    write_csv(
        ('reference_acquisition_month', 'count'),
        output_rows,
        'reference-acquisition-month-counts.csv'
    )



def write_csv(first_row: tuple, rows: list[tuple], output_path: str) -> None:
    with open(output_path, 'w') as f:
        f.write(','.join(first_row) + '\n')
        for row in rows:
            f.write(','.join(map(str, row)) + '\n')


def main():
    with open('deduplicated_pairs.csv') as f:
        lines = f.read().strip('\n').split('\n')

    rows = [tuple(line.split(',')) for line in lines]
    assert len(rows) == 1081

    write_secondary_count_frequencies(rows)
    write_tile_counts(rows)
    write_month_counts(rows)


if __name__ == '__main__':
    main()
