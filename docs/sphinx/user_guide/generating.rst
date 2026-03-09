.. _generating:

***************
Generating Data
***************

TEEHR provides generators for creating synthetic timeseries data, useful for
benchmarking forecasts against baselines or computing climatological normals.

Generator Categories
====================

TEEHR organizes generators into two categories:

- **Signature Timeseries Generators**: Create climatology/normals timeseries from historical data
- **Benchmark Forecast Generators**: Create reference forecasts for skill score comparisons


Signature Timeseries: Normals
=============================

The ``Normals`` generator computes climatological averages from historical data,
producing day-of-year or hour-of-year normals that can be used as baselines.

Basic Usage
-----------

.. code-block:: python

    import teehr
    from teehr import SignatureTimeseriesGenerators as sts

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Configure the normals generator
    normals = sts.Normals()
    normals.temporal_resolution = "day_of_year"  # or "hour_of_year"
    normals.summary_statistic = "mean"           # or "median", "max", "min"

    # Generate normals and write to primary_timeseries
    ev.generate.signature_timeseries(
        method=normals,
        input_table_name="primary_timeseries",
        start_datetime="2023-01-01T00:00:00",
        end_datetime="2024-12-31T00:00:00",
        timestep="1 hour",
        fillna=False,
        dropna=False,
        update_variable_table=True
    ).write()  # Writes to primary_timeseries by default

Configuration Options
---------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``temporal_resolution``
     - Time period for grouping: ``"day_of_year"`` or ``"hour_of_year"``
   * - ``summary_statistic``
     - Aggregation function: ``"mean"``, ``"median"``, ``"max"``, ``"min"``

Generate Method Parameters
--------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``method``
     - The generator instance (e.g., ``sts.Normals()``)
   * - ``input_table_name``
     - Source table for historical data (typically ``"primary_timeseries"``)
   * - ``start_datetime``
     - Start of output timeseries period
   * - ``end_datetime``
     - End of output timeseries period
   * - ``timestep``
     - Output timestep (e.g., ``"1 hour"``, ``"6 hours"``)
   * - ``fillna``
     - Fill NaN values using forward/backward fill
   * - ``dropna``
     - Drop rows with NaN values
   * - ``update_variable_table``
     - Automatically add new variable entries

Example: Daily Normals
----------------------

.. code-block:: python

    import teehr
    from teehr import SignatureTimeseriesGenerators as sts

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Add configuration for USGS observations
    ev.configurations.add(
        teehr.Configuration(
            name="usgs_observations",
            type="primary",
            description="USGS streamflow observations"
        )
    )

    # Load historical USGS data
    ev.primary_timeseries.load_parquet(
        in_path="/path/to/historical_data.parquet"
    )

    # Generate day-of-year mean normals
    normals = sts.Normals()
    normals.temporal_resolution = "day_of_year"
    normals.summary_statistic = "mean"

    ev.generate.signature_timeseries(
        method=normals,
        input_table_name="primary_timeseries",
        start_datetime="2023-01-01T00:00:00",
        end_datetime="2023-12-31T23:00:00",
        timestep="1 hour",
        update_variable_table=True
    ).write()

    # Query the generated normals
    ev.primary_timeseries.filter(
        "variable_name LIKE '%day_of_year_mean%'"
    ).to_sdf().show()


Benchmark Forecasts
===================

Benchmark forecast generators create baseline forecasts for computing skill scores.
These are typically derived from observed climatology or persistence.

Reference Forecast
------------------

The ``ReferenceForecast`` generator creates a benchmark forecast by assigning
historical reference values (e.g., climatology) to forecast timesteps:

.. code-block:: python

    import teehr
    from teehr import BenchmarkForecastGenerators as bm

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Configure reference forecast generator
    ref_fcst = bm.ReferenceForecast()

    # Optional: aggregate reference timeseries
    ref_fcst.aggregate_reference_timeseries = False
    ref_fcst.aggregation_time_window = "6 hours"

    # Add output configuration
    ev.configurations.add(
        teehr.Configuration(
            name="benchmark_forecast_daily_normals",
            type="secondary",
            description="Reference forecast based on USGS climatology"
        )
    )

    # Generate the benchmark forecast
    ev.generate.benchmark_forecast(
        method=ref_fcst,
        reference_table_name="primary_timeseries",
        template_table_name="secondary_timeseries",
        reference_table_filters=[
            "configuration_name = 'usgs_climatology'",
            "variable_name = 'streamflow_hourly_climatology'"
        ],
        template_table_filters=[
            "configuration_name = 'MEFP'",
            "member = '1993'"
        ],
        output_configuration_name="benchmark_forecast_daily_normals"
    ).write(destination_table="secondary_timeseries")

Benchmark Forecast Parameters
-----------------------------

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Parameter
     - Description
   * - ``method``
     - The generator instance (e.g., ``bm.ReferenceForecast()``)
   * - ``reference_table_name``
     - Table containing reference/climatology data
   * - ``template_table_name``
     - Table containing forecast structure to replicate
   * - ``reference_table_filters``
     - Filters to select specific reference data
   * - ``template_table_filters``
     - Filters to select forecast template
   * - ``output_configuration_name``
     - Configuration name for generated benchmarks

Persistence Forecast
--------------------

The ``Persistence`` generator creates forecasts that assume conditions persist
unchanged from the initial time (t=0):

.. code-block:: python

    from teehr import BenchmarkForecastGenerators as bm

    persistence = bm.Persistence()

    # Note: Full implementation pending
    # This will assign t=0 values to all forecast lead times


Complete Workflow
=================

A typical workflow for generating benchmark forecasts:

.. code-block:: python

    import teehr
    from teehr import SignatureTimeseriesGenerators as sts
    from teehr import BenchmarkForecastGenerators as bm

    ev = teehr.Evaluation(dir_path="/path/to/evaluation")

    # Step 1: Load locations and crosswalks
    ev.locations.load_spatial(in_path="locations.parquet")
    ev.location_crosswalks.load_csv(in_path="crosswalk.csv")

    # Step 2: Load primary observations
    ev.configurations.add(
        teehr.Configuration(
            name="usgs_observations",
            type="primary",
            description="USGS streamflow observations"
        )
    )
    ev.primary_timeseries.load_parquet(in_path="usgs_obs.parquet")

    # Step 3: Generate climatological normals
    normals = sts.Normals()
    normals.temporal_resolution = "day_of_year"
    normals.summary_statistic = "mean"

    ev.generate.signature_timeseries(
        method=normals,
        input_table_name="primary_timeseries",
        start_datetime="2020-01-01T00:00:00",
        end_datetime="2023-12-31T23:00:00",
        timestep="1 hour",
        update_variable_table=True
    ).write()

    # Step 4: Load template forecast data
    ev.configurations.add(
        teehr.Configuration(
            name="nwm_forecast",
            type="secondary",
            description="NWM Medium Range Forecast"
        )
    )
    ev.secondary_timeseries.load_parquet(in_path="nwm_forecast.parquet")

    # Step 5: Generate benchmark forecast from normals
    ev.configurations.add(
        teehr.Configuration(
            name="benchmark_climatology",
            type="secondary",
            description="Benchmark forecast from daily normals"
        )
    )

    ref_fcst = bm.ReferenceForecast()
    ev.generate.benchmark_forecast(
        method=ref_fcst,
        reference_table_name="primary_timeseries",
        template_table_name="secondary_timeseries",
        reference_table_filters=[
            "variable_name LIKE '%day_of_year_mean%'"
        ],
        template_table_filters=[
            "configuration_name = 'nwm_forecast'"
        ],
        output_configuration_name="benchmark_climatology"
    ).write(destination_table="secondary_timeseries")

    # Step 6: Create joined view and compute skill scores
    from teehr.metrics import DeterministicMetrics

    metrics_df = ev.joined_timeseries_view().query(
        include_metrics=[
            DeterministicMetrics.KlingGuptaEfficiency(),
            DeterministicMetrics.NashSutcliffeEfficiency(),
        ],
        group_by=["primary_location_id", "configuration_name"],
    ).to_pandas()

    # Compare NWM forecast skill vs. benchmark climatology
    print(metrics_df)

    ev.spark.stop()
