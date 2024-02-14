# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.8] - 2024-02-14

### Added
* Adds logging with a `NullHandler()` that can be implemented by the parent app using teehr.


## [0.3.7] - 2024-02-09

### Changed
* Upgraded pandas to ^2.2.0
* Changed unit="H" in pandas.time_delta to unit="h"
* Updated assert statements in `test_weight_generation.py`

## [0.3.6] - 2024-02-07

### Added
* Adds an exception to catch an error when a corrupted file is encountered while building
the Kerchunk reference file using `SingleHdf5ToZarr`.
* The behavior determining whether to raise an exception is controlled by the
`ignore_missing_file` flag.


## [0.3.5] - 2023-12-18

### Added
* Adds additional chunking methods for USGS and NWM retrospective loading to allow
  week, month and year chunking.
* Adds mean areal summaries for NWM retrospective gridded forcing variables
* Adds NWM v3.0 to retrospective loading

### Changed
* Fixes USGS loading to include last date of range
* Removes extra fields from v2.1 retro output

## [0.3.4] - 2023-12-18

### Added
* Adds the `read_only` argument to the `query` method in the TEEHRDatasetDB class with default values
specified in the query methods.

### Changed
* Establishes a read-only database connection as a class variable to the TEEHRDatasetAPI class so it can
be re-used for each class instance.

## [0.3.3] - 2023-12-13

### Added
* Adds `get_joined_timeseries` method to TEEHR Dataset classes.

### Changed
* Updated validation fields in the `TimeSeriesQuery` pydantic model to accept only selected fields
rather than existing database fields.
* Updated function argument typing in `queries/utils.py` to be more explicit

## [0.3.2] - 2023-12-12

### Added
* None

### Changed
* Fixed the `bias` metric so that it is `sum(secondary_value - primary_value)/count(*)` instead of
  `sum(primary_value - secondary_value)/count(*)` which resulted in the wrong sign.
* Changed `primary_max_value_time`, `secondary_max_value_time` and `max_value_timedelta`
  queries to use built-in functions instead of CTEs.  This improves speed significantly.
* Fixed bug in queries when filtering by `configuration`, `measurement_unit` and `variable.`
* Refactored `join_attributes` in `TEEHRDatasetDB` to better handle attributes with no units.
* Refactored `create_join_and_save_timeseries_query queries` so that the de-duplication
CTE is after the intial join CTE for improved performance.
* Changes default list of `order_by` variables in `insert_joined_timeseries` to improve
query performance


## [0.3.1] - 2023-12-08

### Added
* Adds a boolean flag to parquet-based metric query control whether or not to de-duplicate.
* Adds a test primary timeseries file including duplicate values for testing.

### Changed
* Refactored parquet-based `get_metrics` and `get_joined_timeseries` queries to that so that the de-duplication
CTE is after the intial join CTE for improved performance.


## [0.3.0] - 2023-12-08

### Added
* Adds a dataclass and database that allows preprocessing of joined timeseries and attributes as well as the addition of user defined functions.
* Adds an initial web service API that serves out `timeseries` and `metrics` along with some other supporting data.
* Adds an initial interactive web application using the web service API.

### Changed
* Switches to poetry to manage Python venv
* Upgrades to Pydantic 2+
* Upgrades to Pangeo image `pangeo/pangeo-notebook:2023.09.11`


## [0.2.9] - 2023-12-08

### Added
* Three options related to kerchunk jsons
  * `local` - (default) previous behavior, manually creates the jsons based on GCS netcdf files using Kerchunk's `SingleHdf5ToZarr`. Any locally existing files will be used before creating new jsons from the remote store.
  * `remote` - use pre-created jsons, skipping any that do not exist within the specified time frame.  Jsons are read directly from s3 using fsspec
  * `auto` - use pre-created jsons, creating any that do not exist within the specified time frame
* Adds `nwm_version` (nwm22 or nwm30) and `data_source` (GCS, NOMADS, DSTOR - currently on GCS implemented) as loading arguments

### Changed
* Combines loading modules into one directory `loading/nwm`
* Updates to loading example notebooks
* Updates to loading tests

## [0.2.8] - 2023-11-14

### Added
- NWM v3.0 data loading and configuration models
- Added check for duplicate rows in `get_metrics` and `get_joined_timeseries` queries (#69)
- Added control for overwrite file behavior in loading (#77)
- Significant refactor of the loading libraries
- Added ability to select which retrospective version to download (v2.0 or v2.1) (#80)

### Changed

- Fixed NWM pydantic configurations models for v2.2
- Refactored `models/loading` directory

## [0.2.7] - 2023-09-14

### Added
- More testing to NWM point and grid loading functions

## [0.2.6] - 2023-09-14

### Changed

- Fixed some sloppy bugs in `nwm_grid_data.py`

### Added
- `ValueError` handling when encountering a corrupt zarr json file

## [0.2.5] - 2023-09-11

### Changed

- None

### Added
- Added ability to use holoviz export to TEEHR-HUB:
    - Installed firefox (and a bunch of dependencies) to the Docker container (using apt)
    - Installed selenium and the geckodriver using conda

## [0.2.4] - 2023-08-30

### Changed

- Behavior of loading when encountering missing files
- Renamed field `zone` to `location_id` in `nwm_grid_data.py` and `generate_weights.py`

### Added
- The boolean flag `ignore_missing_files` to point and grid loading to determine whether to fail or continue on missing NWM files
- Added a check to skip locally existing zarr json files when loading NWM data

## [0.2.3] - 2023-08-23

### Changed

- Removed pyarrow from time calculations in `nwm_point_data.py` loading due to windows bug
- Updated output file name in `nwm_point_data.py` to include forecast hour if `process_by_z_hour=False`

## [0.2.2] - 2023-08-23

### Added

- nodejs to the jupyterhub build so the extensions will load (not 100% sure this was needed)

### Changed

- Updated TEEHR to v0.2.2, including TEEHR-HUB
- Updated the TEEHR-HUB baseimage to `pangeo/pangeo-notebook:2023.07.05`

## [0.2.1] - 2023-08-21

### Added

- Nothing

### Changed

- Updated TEEHR version in TEEHR-HUB to v0.2.1
- Converts nwm feature id's to numpy array in loading

## [0.2.0] - 2023-08-17

### Added

- This changelog

### Changed

- Loading directory refactor changed import paths to loading modules
- Changed directory of `generate_weights.py` utility
- Replaced NWM config parameter dictionary with pydantic models
- NWM reference time  used by TEEHR is now taken directly from the file name rather than the "reference time" embedded in the file
- Use of the term `run` updated to `configuration` for NWM

## [0.1.3] - 2023-06-17

### Added

- Initial release