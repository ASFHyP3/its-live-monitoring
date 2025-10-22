# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0]
### Added
- Added Sentinel-1 pair-picking and processing

## [0.5.11]
### Added
- Add `mypy` to static analysis workflow.

## [0.5.10]
### Changed
- Update `ruff` configuration to our latest standards.

## [0.5.9]
### Changed
- Improved deduplication performance by searching HyP3's dynamodb directly for `PENDING` and `RUNNING` jobs, instead of using the `hyp3_sdk`.

### Fixed
- Mixed STAC Item datetime formats (e.g., occasionally not including microseconds) in the list of secondary scenes no longer causes a ValueError to be raised. 

## [0.5.8]
### Changed
- As an incremental improvement to deduplication performance, its-live-monitoring now:
  - searches the `s3://its-live-data` bucket directly for already published (succeeded) pairs.
  - searches HyP3 ITS_LIVE via the API for pairs still pending or running, instead of searching for all previously submitted pairs.
- Upgrade numpy from 1.26.4 to 2.1.3

## [0.5.7]
### Fixed
- Normalized the Sentinel-2 tile list to match the Element84 STAC representation, fixing tiles with a leading `0` being excluded from processing.

## [0.5.6]
### Changed
- Reduced SQS batch size from 10 to 1 so that each de-duplication attempt has up to the full 15-minute Lambda timeout.

## [0.5.5]
### Fixed
- Reduced maximum concurrent executions of the `its_live_monitoring` lambda from 1,000 to 100 to reduce the frequency of
  `hyp3_sdk.exceptions.ServerError` exceptions when de-duplicating new jobs. See [#119](https://github.com/ASFHyP3/its-live-monitoring/issues/119).

## [0.5.4]
### Fixed
- Convert the `FilterPolicy` property of the `LandsatSubscription` CloudFormation resource from JSON to YAML, to allow upgrading to `cfn-lint` v1.3.4 (see <https://github.com/aws-cloudformation/cfn-lint/issues/3403>).
- Disqualify Sentinel-2 scenes from reprocessing campaigns before querying the STAC catalog.

## [0.5.3]
### Fixed
- Downgraded the HyP3 SDK to v6.1.0 from v6.2.0 due to timeouts related to checking user's application status, see [ASFHyP3/hyp3-sdk#280](https://github.com/ASFHyP3/hyp3-sdk/issues/280). 

## [0.5.2]

### Changed
- Sentinel-2 products are now disqualified from processing if they do not have enough data coverage.
- Sentinel-2 products are now disqualified from processing if the secondary scene's relative orbit does not match that of the reference scene.
- Switched from Dataspace's Sentinel-2 STAC API to Element84's.

## [0.5.1]

### Fixed
- Sentinel-2 search geometry now uses a small central square within a tile instead of a tile's bbox to avoid finding images from neighboring tiles.
- its-live-monitoring now deploys the Sentinel-2 SNS subscription to the `eu-west-1` region since subscriptions are required to be in the same regions as the SNS Topic.

## [0.5.0]

**NOTE:** Failed to deploy.

### Added
- Support for processing Sentinel-2 SNS messages and submitting jobs to hyp3-its-live has been added

### Changed
- To manage any lag between Sentinel-2 messages being published in AWS and scenes being available in Google Cloud, which is where hyp3-autorift pulls scenes from, the message failure handling has been changed:
  - The visibility timeout (time between attempts) has been extended from 5 minutes to 8 hours
  - Processing messages will be attempted 3 times before being driven to the dead letter queue
- The `its_live_monitoring` lambda timeout has been increased to 900 seconds, from 300 seconds, because pair picking for Sentinel-2 takes significantly longer due to the Copernicus Dataspace STAC catalog not supporting metadata queries  

## [0.4.0]

### Added
- Off-nadir scenes will now be processed and will only be paired with other off-nadir scenes.

## [0.3.0]

### Added
- A CLI wrapper for `status_messages.py` so that it can more easily be run locally.

### Fixed
- Status messages can now be posted to mattermost with a bot account.

## [0.2.0]

### Added
- Dead-letter queue count is now posted automatically to Mattermost.

## [0.1.0]

### Changed
- HyP3 jobs will now be submitted with the `publish_bucket` job parameter set
- The reason a scene disqualifies for processing will now be logged

### Fixed
- The `landsat:cloud_cover_land` property instead of `eo:cloud_cover` will be used to determine if a scene qualifies for processing
- Scenes with unknown cloud cover (unreported or a value < 0) will be disqualified for processing
- The max cloud cover percentage is now an inclusive bound, so only scenes with *more* (`>`) cloud cover will be disqualified 

## [0.0.3]

### Changed
- This application will now monitor for newly-published Landsat 8/9 scenes over all land-ice intersecting Landsat tiles


## [0.0.2]

### Changed
- Limited the SNS subscription for Landsat to T1 and T2 scenes to filter out RT scenes earlier in the workflow.

## [0.0.1]

### Added
- Initial release of its-live-monitoring. The application will monitor for newly-published Landsat 8/9 scenes over 50
  Landsat tiles and submit a stack of AUTORIFT jobs for each to hyp3-its-live.asf.alaska.edu for processing.
