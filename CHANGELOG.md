# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


##  [0.0.4]

### Changed
- HyP3 jobs will now be submitted with the `publish_bucket` job parameter set

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
