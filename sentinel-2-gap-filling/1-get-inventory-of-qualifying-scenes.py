import json
from datetime import datetime

import sentinel2


results = sentinel2.SENTINEL2_CATALOG.search(
    collections=[sentinel2.SENTINEL2_COLLECTION_NAME],
    query={
        'eo:cloud_cover': {
            'lte': sentinel2.SENTINEL2_MAX_CLOUD_COVER_PERCENT,
        },
        'grid:code': {
            'in': [f'MGRS-{tile}' for tile in sentinel2.SENTINEL2_TILES_TO_PROCESS],
        },
    },
    datetime=[datetime(2024, 7, 31, hour=23), datetime(2024, 8, 1)],
)
items = []
for ii, page in enumerate(results.pages()):
    print(ii)
    items.extend([item.to_dict() for item in page if sentinel2.qualifies_for_sentinel2_processing(item)])

with open('all_qualifying_s2_scenes.json', 'w') as f:
    json.dump(items, f)
