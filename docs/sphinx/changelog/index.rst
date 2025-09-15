Release Notes
=============

0.5.1 - 2025-09-15
-------------------

Changed
^^^^^^^
- Fixed USGS fetching for daily values when `service=dv`

0.5.0 - 2025-08-27
-------------------

Changed
^^^^^^^
- Many updates and new features are introduced in this release, including but not limited to:
  - Baseflow separation methods
  - Better handling of warnings
  - Ability to generate benchmark timeseries and forecasts
  - Upgrading to pyspark 4.0
  - Documentation updates
- For a full list see: https://github.com/RTIInternational/teehr/milestone/4?closed=1

0.4.13 - 2025-06-09
-------------------

Changed
^^^^^^^
- Updates logic around reading empty tables to allow for cloning empty timeseries tables from s3. Overrides the read method in the `joined_timeseries` table class.
- Removes the `pa.Check.isin()` pandera validation checks and replaces with the manual "foreign key" enforcement method, `_enforce_foreign_keys()` to speed up validation.
- Removes redundant dataframe validation during the write methods.
- Sets `.set("spark.sql.parquet.enableVectorizedReader", "false")` in pyspark config to fix type error when reading null parquet fields.
- Fixes the path conversion error when visualizing locations in the accessor.
- Sorts timeseries before plotting to fix the visualization error in the accessor.
- Removes `add_configuration_name` from user guide doc (#450)
- Adds `tomli` to pyproject.toml to `dev` group to support python 3.12 when building sphinx docs.
- Drops `location_id` after joining attributes to the `joined_timeseries` table.
- Fixes joining geometry to the secondary timeseries table.
- Sets `timeseries_type` to `secondary` when fetching operational NWM gridded data, unless the configuration_name contains "forcing_analysis_assim" in which case `timeseries_type` is set to `primary`.


0.4.12 - 2025-05-22
------------------

Changed
^^^^^^^
- Adds `git` and `vim` to docker image for TEEHR-HUB
- Moves `scoringrules` and `arch` imports into the function to speed up import time
- Removes the repartitioning by `self.partition_by` of the dataframe in the `BaseTable` class when writing to parquet

0.4.11 - 2025-05-19
------------------

Changed
^^^^^^^
- Fixes bug in _write_spark_df() method in the BaseTable class that caused writing larger dataframes to fail.
- Parallelizes `convert_single_timeseries()` when a directory is passed to the `in_path` argument.
- Fixes doc string in `generate_weights_file()`
- Switched to the built-in dropDuplicates() method in the `BaseTable` class to drop duplicates instead of using a custom implementation.
- Added option to specify the number of partitions when writing dataframes in the BaseTable class.
- Added the option to skip the dropDuplicates() method when writing dataframes in the BaseTable class.

0.4.10 - 2025-04-14
------------------

Added
^^^^^
- Adds append and upsert functionality without duplicates to loading methods on tables:
    - locations
    - location crosswalk
    - location attributes
    - primary and secondary timeseries
- Adds upsert argument to fetching methods (append is default).
- Clears fetching cache before each call.
- Adds ability to add or update the location id prefix during loading in above tables
- Adds `reference_time` as a default partition for secondary and joined timeseries
- Adds script to re-write timeseries tables partitioned on `reference_time`
- Adds function to drop potential duplicates before writing tables (`_drop_duplicates()`)
- Combines the script to calculate pixel weights per polygon (`generate_weights.py`) with the NWM gridded Evaluation fetching methods (retro and operational).
    - This allows users to optionally generate the weights from within the fetching methods or to use a pre-created weights file.
    - When run from the Evaluation, the weights file is saved to the evaluation cache and corresponds to ids in the `locations` table.
- Adds User Guide notebook for NWM gridded fetching
- Adds transform functions to metric calculations
- Adds `geoviews` dependency to poetry evaluation
- Adds aws cli and `datashader` to the TEEHR-HUB docker image
- Removes `duckdb` for teehr env


0.4.9 - 2025-03-26
------------------

Added
^^^^^
- Adds pandera schema for the weights file and validates weights dataframe on read and write, coercing values into schema data types
- Adds ``starting_z_hour`` and ``ending_z_hour`` arguments to operational NWM fetching methods (point, gridded)
- Adds function to drop NaN values (from value field) when fetching NWM and USGS data
- Adds a check so that if schema validation fails, the current file is skipped and fetching continues
- Adds versions 1.2 and 2.0 to operational NWM fetching (version 2.2 (nwm22) is allowed to be used with a note that it is no different from 2.1)
- Adds a test notebook for testing on remote teehr-hub kernel
- Adds wrapper functions for deterministic and signature metrics


Changed
^^^^^^^
- Fixes doc strings for fetch.nwm_retrospective_grids()
- Removes ``add_configuration_name`` in fetching and automatically adds if it doesn't exist
- Updates dask version
- Fixes a bug in parsing the z_hour and day from the remote json paths when an ensemble configuration is selected
- Removes the imports in ``__init__.py`` that were for documentation purposes
- Removes hydrotools as a dependency
- Updates API documentation, adding evaluation.metrics.Metrics methods
- Changes base docker image to ``base-notebook:2025.01.24``


0.4.8 - 2025-02-17
------------------

Added
^^^^^
- Adds box zoom to location plots.
- Adds User Guide page for fetching NWM point data.
- Adds new row level calculated fields, DayOfYear, ThresholdValueExceeded, ForecastLeadTime.


Changed
^^^^^^^
- Changes NWM fetching methods from ``nwm_forecast_<xxxx>`` to ``nwm_operational_<xxxx>``.

- Set ``use_table_schema`` to False when cloning the ``joined_timeseries`` table from s3,
  so that extra fields will not be dropped. Note, this will raise an error if the table is
  empty or does not exist.

- Made auto-adding of configuration_name in NWM and USGS fetching optional.

- Removed 2 evaluations from s3 (HEFS, NWM fetching), using TEEHR data module instead.


0.4.7 - 2025-01-08
------------------

Added
^^^^^
* Adds RowLevelCalculatedFields and TimeseriesAwareCalculatedFields which are hopefully descriptive enough names.

* Adds a User Guide page to describe what they are and how to use them.

* Adds hvplot dependency to poetry

* Adds add_calculated_fields() methods to joined_timeseries and metrics "tables"

* Adds the Continuous Rank Probability Score CRPS ensemble metric using the scoringrules package

* Adds a script to create an example ensemble evaluation using data in the test directory

* Adds an example notebook to demo CRPS metric query

* Adds user guide notebook page for ensembles, reading a test ensemble evaluation from S3

* Adds ability to unpack metric dictionary results into separate columns (ie, bootstrap quantiles)

Changed
^^^^^^^
* Splits metric models and functions into three categories: Deterministic, Probabilistic, Signature. This is a breaking change requiring import of specific metric classes (Deterministic, Probabilistic, Signature) rather than just ``Metrics``.

  * Functions are moved to separate modules

  * Models are moved to separate classes

  * Basemodels and metric enums are moved to a separate basemodel module

* Updates API docs, removes unused files and the autoapi directory.


0.4.6 - 2024-12-17
--------------------

Added
^^^^^
* Adds `add_missing_columns` to the `_validate` method in the `BaseTable` class
  to allow for adding missing columns to the schema.

* When upgrading from 0.4.4 or earlier, you may need to run the following to add
  the missing columns to the secondary_timeseries if you have existing datasets:

.. code-block:: python

  sdf = ev.secondary_timeseries.to_sdf()
  validated_sdf = ev.secondary_timeseries._validate(sdf, add_missing_columns=True)
  ev.secondary_timeseries._write_spark_df(validated_sdf)

Changed
^^^^^^^
* None


0.4.5 - 2024-12-09
--------------------

Added
^^^^^
* Fixes issues with sphinx docs and run the `install_spark_jars.py` script in the build container.
* Adds location plotting to accessor.
* Adds loading from FEWS XML files.
* Adds `member` to secondary timeseries schema for ensembles.

Changed
^^^^^^^
* Fixes issues with sphinx docs and run the `install_spark_jars.py` script in the build container.

0.4.4 - 2024-12-02
--------------------

Added
^^^^^
* Added ability to read an Evaluation dataset directly from an S3 bucket.
* When path to an Evaluation dataset is an S3 bucket, the Evaluation is read-only.

Changed
^^^^^^^
* Pretty significant refactor of the Table classes to make them more flexible and easier to use.
* Added more robust Pandera validation to the Table classes.
* Updated docs to reflect changes and added `read_from_s3` example.


0.4.3 - 2024-10-19
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Changed paths to the S3 bucket evaluations to reference "e*..." instead of "p*..." naming convention.

0.4.2 - 2024-10-18
--------------------

Added
^^^^^
* A test-build-publish workflow to push to PyPI

Changed
^^^^^^^
* None

0.4.1 - 2024-10-15
--------------------

Added
^^^^^
* Updated docs to include pages for `grouping`, `filtering` and `Joining` in the User Guide.

Changed
^^^^^^^
* Fixed some broken data download links in the User Guide.
* Fixed the post-install script to install the AWS Spark Jars.
* Fixed the API doc build.

0.4.0 - 2024-10-13
--------------------

Added
^^^^^
* This is a major (although still less that version 1) release that includes a number of new features and changes.
* Some of the more significant changes:
  - Added a new Evaluation class that is the primary interface for working with TEEHR data.
  - Switched from DuckDB to PySpark to enable horizonal scaling for the computational workloads.
  - Formalized the structure of the TEEHR dataset.
  - Added data validation of values referenced from domain and location tables to the timeseries tables.
  - Updated docs to include new features and changes.

Changed
^^^^^^^
* Many changes have been made between v0.3.28 and v0.4.0.

0.3.28 - 2024-07-10
--------------------

Added
^^^^^
* pandas DataFrame accessor classes for metrics and timeseries queries, including some simple methods
  for plotting and summarizing data.
* Added Bokeh as a dependency for visualization.

Changed
^^^^^^^
* None


0.3.27 - 2024-07-08
-------------------

Added
^^^^^
* Documentation updates primarly to Getting Started and User Guide sections.

Changed
^^^^^^^
* None


0.3.26 - 2024-06-27
--------------------

Added
^^^^^
* Dark theme logo for sphinx documentation.
* Added the `pickleshare` package to dev dependency group to fix `ipython` directive in sphinx documentation.

Changed
^^^^^^^
* Pinned `sphinx-autodoc` to v3.0.0 and `numpy` to v1.26.4 in `documentation-publish.yml` to fix the API documentation build.
* Removed unused documentation dependencies from dev group.


0.3.25 - 2024-06-06
--------------------

Added
^^^^^
* Added PySpark to TEEHR-HUB (including openjdk-17-jdk and jar files)

Changed
^^^^^^^
* None


0.3.24 - 2024-05-29
--------------------

Added
^^^^^
* Added metrics documentation to the Sphinx documentation.

Changed
^^^^^^^
* None


0.3.23 - 2024-05-28
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Docstring updates in duckdb_database.py.
* Changelog update for 0.3.22.
* Updates ``insert_attributes()`` in ``duckdb_database.py`` to better handle None/Null attribute units.
* Test updates in ``convert.py``.


0.3.22 - 2024-05-22
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Cleaned up the `DuckDB*` classes.  Don't think any public interfaces changed.
* Import of `DuckDBDatabase`, `DuckDBDatabaseAPI`, and `DuckDBJoinedParquet`
  now use `from teehr.classes import DuckDBDatabase, DuckDBDatabaseAPI, DuckDBJoinedParquet`
* the `calculate_field`` method was renamed to `insert_calculated_field``


0.3.21 - 2024-05-21
--------------------

Added
^^^^^
* Added the ``DuckDBJoinedParquet`` class for metric queries on pre-joined parquet files.
* Added the ``DuckDBBase`` class for common methods between the ``DuckDBDatabase``, ``DuckDBAPI``,
  and ``DuckDBJoinedParquet`` classes.

Changed
^^^^^^^
* Renamed the ``database`` directory to ``classes``.
* Renamed the ``teehr_dataset.py`` to ``teehr_duckdb.py``.
* Renamed the ``TEEHRDatasetDB`` and ``TEEHRDatasetAPI`` classes to
  ``DuckDBDatabase`` and ``DuckDBAPI`` respectively.
* Removed `lead_time` and `absolute_value` from joined table


0.3.20 - 2024-05-18
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Update queries to accept a list of paths for example, `primary_filepath` and `secondary_filepath`
  Includes `get_metrics()`, `get_joined_timeseries()`, `get_timeseries()`, and `get_timeseries_chars()`


0.3.19 - 2024-05-18
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Update SQL queries to allow `reference_time` to be NULL.
* Updated tests for NULL `reference_time`


0.3.18 - 2024-05-10
--------------------

Added
^^^^^
* Added documentation regarding best practices for specifying the ``chunk_by`` parameter when fetching NWM
  retrospective and USGS data.

Changed
^^^^^^^
* Fixed a bug in the NWM retrospective grid loading weighted average calculation.
* Changed the method of fetching NWM gridded data to read only a subset of the grid (given by the row/col
  bounds from the weights file) into memory rather than the entire grid.
* Removed 'day' and 'location_id' ``chunk_by`` options to reduce redundant data transfer costs.


0.3.17 - 2024-04-22
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Dropped "Z" from the file name in the NWM loading functions, adding a note in the docstrings that all times are in UTC.
* Changed data type of ``zonal_weights_filepath`` to ``Union[str, Path]`` in ``nwm_grids.py``.
* Fixed ``SettingWithCopyWarning`` in NWM grid loading.
* Fixed the ``end_date`` in NWM retrospective loading to include the entirety of the last day and not fail when
  last available day is specfified.
* Removed "elevation", "gage_id", "order" from NWM v3.0 retrospective point loading.


0.3.16 - 2024-04-11
--------------------

Added
^^^^^
* Adds a few new metrics to the queries:
  * annual_peak_relative_bias
  * spearman_correlation
  * kling_gupta_efficiency_mod1
  * kling_gupta_efficiency_mod2

Changed
^^^^^^^
* None

0.3.15 - 2024-04-08
--------------------

Added
^^^^^
* ``location_id_prefix`` as an optional argument to ``generate_weights_file()`` to allow for
  the prefixing of the location ID with a string.

Changed
^^^^^^^
* Updated the NWM operational and retrospective grid loading functions so that the location ID
  as defined in the zonal weights file is used as the location ID in the output parquet files.

0.3.14 - 2024-03-29
--------------------

Added
^^^^^
* relative_bias
* multiplicative_bias
* mean_squared_error
* mean_absolute_relative_error
* pearson_correlation
* r_squared
* nash_sutcliffe_efficiency_normalized

Changed
^^^^^^^
* mean_error (rename current bias to mean_error)
* mean_absolute_error (rename current mean_error to mean_absolute_error)

0.3.13 - 2024-03-22
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Updated from Enum to StrEnum and added a fix for backwards incompatibility described
  here: https://tomwojcik.com/posts/2023-01-02/python-311-str-enum-breaking-change.  This
  is required to support both python 3.10 and python 3.11.
* Updated TEEHR-HUB to Python 3.11 and `pangeo/pangeo-notebook:2024.03.13`
* Made all packages that use YYYY.MM.DD versioning `>=` instead of `^` in `pyproject.toml`


0.3.12 - 2024-03-22
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Changed the chunking method for USGS and NWM retrospective data loading to iterate over pandas ``period_range``
  rather than using ``groupby`` or ``date_range`` to fix a bug when fetching data over multiple years.

0.3.11 - 2024-03-19
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
* Downgraded required Dask version to `dask = "^2023.8.1"` to match `pangeo/pangeo-notebook:2023.09.11`

0.3.10 - 2024-03-07
--------------------

Added
^^^^^
* Added `test_zonal_mean_results.py`

Changed
^^^^^^^
* Fixed the calculation of the zonal mean of pixel values in `compute_zonal_mean()` so it caculates
  the weighted average (divides by the sum of weight values).
* Updated grid loading tests and data to reflect the fixed method.

0.3.9 - 2024-02-15
--------------------

Added
^^^^^
* Adds sphinx documentation framework and initial docs.
* The `documentation-publish.yml` workflow is set to build the docs and push to github pages
  on every tag.
* The `pre-commit-config.yml` github hook runs on each commit and checks docstring formatting,
  trailing whitespaces, and the presence of large files.
* Added documenation-related python dependencies to `[tool.poetry.group.dev.dependencies]`

Changed
^^^^^^^
* Example notebooks have been moved to `docs/sphinx/user_guide/notebooks`.
* The CHANGELOG.md is now the `index.rst` file in `docs/sphinx/changelog`.
* The CONTRIBUTE.md and release_process.md files now part of the `index.rst`
  file in `docs/sphinx/development`.
* The data_models.md and queries.md are now the `data_models.rst` and `queries.rst`
  files in `docs/sphinx/getting_started`.


0.3.8 - 2024-02-14
--------------------

Added
^^^^^
* Adds logging with a `NullHandler()` that can be implemented by the parent app using teehr.


0.3.7 - 2024-02-09
--------------------

Changed
^^^^^^^
* Upgraded pandas to ^2.2.0
* Changed unit="H" in pandas.time_delta to unit="h"
* Updated assert statements in `test_weight_generation.py`

0.3.6 - 2024-02-07
--------------------

Added
^^^^^
* Adds an exception to catch an error when a corrupted file is encountered while building
  the Kerchunk reference file using `SingleHdf5ToZarr`.
* The behavior determining whether to raise an exception is controlled by the
  `ignore_missing_file` flag.


0.3.5 - 2023-12-18
--------------------

Added
^^^^^
* Adds additional chunking methods for USGS and NWM retrospective loading to allow
  week, month and year chunking.
* Adds mean areal summaries for NWM retrospective gridded forcing variables
* Adds NWM v3.0 to retrospective loading

Changed
^^^^^^^
* Fixes USGS loading to include last date of range
* Removes extra fields from v2.1 retro output

0.3.4 - 2023-12-18
--------------------

Added
^^^^^
* Adds the `read_only` argument to the `query` method in the TEEHRDatasetDB class with default values
  specified in the query methods.

Changed
^^^^^^^
* Establishes a read-only database connection as a class variable to the TEEHRDatasetAPI class so it can
  be re-used for each class instance.

0.3.3 - 2023-12-13
--------------------

Added
^^^^^
* Adds `get_joined_timeseries` method to TEEHR Dataset classes.

Changed
^^^^^^^
* Updated validation fields in the `TimeSeriesQuery` pydantic model to accept only selected fields
  rather than existing database fields.
* Updated function argument typing in `queries/utils.py` to be more explicit

0.3.2 - 2023-12-12
--------------------

Added
^^^^^
* None

Changed
^^^^^^^
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

0.3.1 - 2023-12-08
--------------------

Added
^^^^^
* Adds a boolean flag to parquet-based metric query control whether or not to de-duplicate.
* Adds a test primary timeseries file including duplicate values for testing.

Changed
^^^^^^^
* Refactored parquet-based `get_metrics` and `get_joined_timeseries` queries to that so that the de-duplication
  CTE is after the intial join CTE for improved performance.


0.3.0 - 2023-12-08
--------------------

Added
^^^^^
* Adds a dataclass and database that allows preprocessing of joined timeseries and attributes as well as the addition of user defined functions.
* Adds an initial web service API that serves out `timeseries` and `metrics` along with some other supporting data.
* Adds an initial interactive web application using the web service API.

Changed
^^^^^^^
* Switches to poetry to manage Python venv
* Upgrades to Pydantic 2+
* Upgrades to Pangeo image `pangeo/pangeo-notebook:2023.09.11`


0.2.9 - 2023-12-08
--------------------

Added
^^^^^
* Three options related to kerchunk jsons
  * `local` - (default) previous behavior, manually creates the jsons based on GCS netcdf files using Kerchunk's `SingleHdf5ToZarr`. Any locally existing files will be used before creating new jsons from the remote store.
  * `remote` - use pre-created jsons, skipping any that do not exist within the specified time frame.  Jsons are read directly from s3 using fsspec
  * `auto` - use pre-created jsons, creating any that do not exist within the specified time frame
* Adds `nwm_version` (nwm22 or nwm30) and `data_source` (GCS, NOMADS, DSTOR - currently on GCS implemented) as loading arguments

Changed
^^^^^^^
* Combines loading modules into one directory `loading/nwm`
* Updates to loading example notebooks
* Updates to loading tests

0.2.8 - 2023-11-14
--------------------

Added
^^^^^
- NWM v3.0 data loading and configuration models
- Added check for duplicate rows in `get_metrics` and `get_joined_timeseries` queries (#69)
- Added control for overwrite file behavior in loading (#77)
- Significant refactor of the loading libraries
- Added ability to select which retrospective version to download (v2.0 or v2.1) (#80)

Changed
^^^^^^^

- Fixed NWM pydantic configurations models for v2.2
- Refactored `models/loading` directory

0.2.7 - 2023-09-14
--------------------

Added
^^^^^
- More testing to NWM point and grid loading functions

0.2.6 - 2023-09-14
--------------------

Changed
^^^^^^^

- Fixed some sloppy bugs in `nwm_grid_data.py`

Added
^^^^^
- `ValueError` handling when encountering a corrupt zarr json file

0.2.5 - 2023-09-11
--------------------

Changed
^^^^^^^

- None

Added
^^^^^
- Added ability to use holoviz export to TEEHR-HUB:
    - Installed firefox (and a bunch of dependencies) to the Docker container (using apt)
    - Installed selenium and the geckodriver using conda

0.2.4 - 2023-08-30
--------------------

Changed
^^^^^^^

- Behavior of loading when encountering missing files
- Renamed field `zone` to `location_id` in `nwm_grid_data.py` and `generate_weights.py`

Added
^^^^^
- The boolean flag `ignore_missing_files` to point and grid loading to determine whether to fail or continue on missing NWM files
- Added a check to skip locally existing zarr json files when loading NWM data

0.2.3 - 2023-08-23
--------------------

Changed
^^^^^^^

- Removed pyarrow from time calculations in `nwm_point_data.py` loading due to windows bug
- Updated output file name in `nwm_point_data.py` to include forecast hour if `process_by_z_hour=False`

0.2.2 - 2023-08-23
--------------------

Added
^^^^^

- nodejs to the jupyterhub build so the extensions will load (not 100% sure this was needed)

Changed
^^^^^^^

- Updated TEEHR to v0.2.2, including TEEHR-HUB
- Updated the TEEHR-HUB baseimage to `pangeo/pangeo-notebook:2023.07.05`

0.2.1 - 2023-08-21
--------------------

Added
^^^^^

- Nothing

Changed
^^^^^^^

- Updated TEEHR version in TEEHR-HUB to v0.2.1
- Converts nwm feature id's to numpy array in loading

0.2.0 - 2023-08-17
--------------------

Added
^^^^^

- This changelog

Changed
^^^^^^^

- Loading directory refactor changed import paths to loading modules
- Changed directory of `generate_weights.py` utility
- Replaced NWM config parameter dictionary with pydantic models
- NWM reference time  used by TEEHR is now taken directly from the file name rather than the "reference time" embedded in the file
- Use of the term `run` updated to `configuration` for NWM


0.1.3 - 2023-06-17
--------------------

Added
^^^^^

- Initial release
