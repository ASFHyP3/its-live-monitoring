with open('all_qualifying_pairs.csv') as f:
    all_pairs = {line.strip() for line in f}

with open('already_processed_pairs.csv') as f:
    already_processed_pairs = {line.strip() for line in f}

deduplicated_pairs = all_pairs - already_processed_pairs
with open('deduplicated_pairs.csv', 'w') as f:
    f.write('\n'.join(deduplicated_pairs))
