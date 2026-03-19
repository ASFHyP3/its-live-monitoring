"""ITS_LIVE monitoring environment variable configuration."""

import os


def _nullable_str(s: str) -> str | None:
    s = s.replace('None', '').strip()
    return s if s else None


def _string_is_true(s: str) -> bool:
    return s.lower() == 'true'


LOGGING_LEVEL = os.environ.get('LOGGING_LEVEL', 'INFO')

# needed to deduplicate via HyP3
HYP3_JOBS_TABLE_NAME = os.environ.get('JOBS_TABLE_NAME', '')
EARTHDATA_USERNAME = os.environ.get('EARTHDATA_USERNAME')

# needed to deduplicate via STAC
STAC_ITEMS_ENDPOINT = _nullable_str(os.environ.get('STAC_ITEMS_ENDPOINT', 'None'))

# needed to submit HyP3 jobs
HYP3_API = os.environ.get('HYP3_API', '')
EARTHDATA_PASSWORD = os.environ.get('EARTHDATA_PASSWORD')
STAC_EXISTS_OK = _string_is_true(os.environ.get('STAC_EXISTS_OK', 'False'))
PUBLISH_BUCKET = _nullable_str(os.environ.get('PUBLISH_BUCKET', 'None'))
