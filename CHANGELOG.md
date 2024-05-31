# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.2]

### Added
- Sentinel-2 products will now be disqualified from processing if they do not have enough data coverage.
### Changed
- Switched from Dataspace's Sentinel-2 STAC API to Element84's

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
