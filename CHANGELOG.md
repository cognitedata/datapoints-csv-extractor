# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## Unpublished
### Added
- Command line argument for specifying if we should move finished files to subfolder

### Changed
- Upgraded dependencis of Cognite sdk and gcloud logger
- Check for new files every 8 seconds in live mode

### Removed
- Removed timestamp command line argument

## [0.1.5] - 2019-05-29
### Added
- Command line argument for setting log level

### Changed
- Historical also works on newest files first
- Larger pool threads of requests to send to CDF

### Fixed
- Timestamps are in milliseconds again
- We sent empty list for all but the last request, fixed by copy

## [0.1.4] - 2019-05-02
### Changed
- Process files with csv library instead of pandas, and post data in parallel after file is processed

### Removed
- Pandas dependency

## [0.1.3] - 2019-04-25
### Added
- Metrics on number of files that we failed to process

## [0.1.2] - 2019-04-17
### Changed
- Moved live/historical distinction from Prometheus label to metric name

## [0.1.1] - 2019-04-17
### Added
- New metrics for files
- Cognite Fusion project name to metrics labels
- Project name to Prometheus metrics

### Changed
- Replaced metrics for datapoints per timeseries with count of timeseries

## [0.1.0] - 2019-04-01
### Added
- LICENSE file
- CHANGELOG.md file
- CONTRIBUTING.md file
- Dockerfile for building a docker image
- Use py.test for unittesting

### Changed
- Updated README to include versioning information
- Logging of missing files as debug log instead of errors

## [0.0.0] - 2019-02-05
### Added
- Initial commit
