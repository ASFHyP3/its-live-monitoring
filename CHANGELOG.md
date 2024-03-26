# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


##  [0.1.0]

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
