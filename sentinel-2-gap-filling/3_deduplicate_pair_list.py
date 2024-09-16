def remove_processing_dates(pair: str) -> str:
    return ','.join('_'.join(name.split('_')[:-1]) for name in pair.split(','))

with open('all_qualifying_pairs.csv') as f:
    all_pairs = {line.strip() for line in f}

with open('already_processed_pairs.csv') as f:
    already_processed_pairs = {remove_processing_dates(line.strip()) for line in f}

deduplicated_pairs = {pair for pair in all_pairs if remove_processing_dates(pair) not in already_processed_pairs}
with open('deduplicated_pairs.csv', 'w') as f:
    f.write('\n'.join(deduplicated_pairs))
