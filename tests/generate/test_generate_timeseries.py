"""Testing utilities for generating synthetic time series data."""
from pathlib import Path

import pytest

import teehr
from teehr import SignatureTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bm


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_warehouse_data")


@pytest.mark.function_scope_evaluation_template
def test_generate_timeseries_normals(function_scope_evaluation_template):
    """Generate synthetic time series data."""
    ev = function_scope_evaluation_template

    usgs_location = Path(
        TEST_STUDY_DATA_DIR,
        "geo",
        "USGS_PlatteRiver_FakeNWM_locations.parquet"
    )
    ev.locations.load_spatial(
        in_path=usgs_location
    )
    ev.configurations.add(
        [
            teehr.Configuration(
                name="usgs_observations",
                timeseries_type="primary",
                description="USGS streamflow observations"
            )
        ]
    )
    ev.configurations.add(
        [
            teehr.Configuration(
                name="nwm30_medium_range_forcing",
                timeseries_type="primary",
                description="Synthetic NWM 30 Medium Range AnA Forcing"
            )
        ]
    )
    ev.primary_timeseries.load_parquet(
        in_path=Path(
            TEST_STUDY_DATA_DIR,
            "timeseries",
            "usgs_hefs_06711565_2yrs.parquet"
        )
    )
    ev.primary_timeseries.load_parquet(
        in_path=Path(
            TEST_STUDY_DATA_DIR,
            "timeseries",
            "synthetic_nwm_forcing_obs_2yrs.parquet"
        )
    )

    ts_normals = sts.Normals()
    ts_normals.temporal_resolution = "day_of_year"  # the default
    ts_normals.summary_statistic = "mean"           # the default

    ev.generate.signature_timeseries(
        method=ts_normals,
        input_table_name="primary_timeseries",
        start_datetime="2023-01-01T00:00:00",
        end_datetime="2024-12-31T00:00:00",
        timestep="1 hour",
        fillna=False,
        dropna=False,
        update_variable_table=True
    ).write()  # default destination: "primary_timeseries"

    prim_df = (
        ev
        .primary_timeseries
        .filter([
            "configuration_name = 'usgs_observations'",
            "variable_name = 'streamflow_hourly_inst'"
        ])
        .to_pandas()
    )
    clim_df = (
        ev
        .primary_timeseries
        .filter([
            "configuration_name = 'usgs_observations'",
            "variable_name = 'streamflow_day_of_year_mean'"
        ])
        .to_pandas()
    )
    # Manually calculate the mean for each day of the year.
    # Leap day of year gets set as the previous day's value.
    prim_df["day_of_year"] = prim_df.value_time.dt.dayofyear
    prim_df["year"] = prim_df.value_time.dt.year
    leap_day_mask = (prim_df.year == 2024) & (prim_df.day_of_year == 60)
    following_leap_day_mask = (prim_df.year == 2024) & \
        (prim_df.day_of_year >= 61)
    prim_df.loc[leap_day_mask, "day_of_year"] = 59
    prim_df.loc[following_leap_day_mask, "day_of_year"] -= 1
    mean_prim_srs = prim_df.copy().groupby("day_of_year")["value"].mean()
    # Check that the climatology matches the manual calculation.
    clim_df["day_of_year"] = clim_df.value_time.dt.dayofyear

    assert clim_df[(clim_df.day_of_year == 59) & (clim_df.value_time.dt.year == 2023)].value.values[0] == mean_prim_srs.loc[59]
    assert clim_df[(clim_df.day_of_year == 60) & (clim_df.value_time.dt.year == 2023)].value.values[0] == mean_prim_srs.loc[60]

    assert clim_df[(clim_df.day_of_year == 59) & (clim_df.value_time.dt.year == 2024)].value.values[0] == mean_prim_srs.loc[59]
    assert clim_df[(clim_df.day_of_year == 60) & (clim_df.value_time.dt.year == 2024)].value.values[0] == mean_prim_srs.loc[59]
    assert clim_df[(clim_df.day_of_year == 61) & (clim_df.value_time.dt.year == 2024)].value.values[0] == mean_prim_srs.loc[60]


@pytest.mark.function_scope_evaluation_template
@pytest.mark.skip(reason="This one causes subsequent tests in test_import_timeseries.py to fail, not sure why yet.")
def test_generate_reference_forecast(function_scope_evaluation_template):
    """Test the reference forecast calculation."""
    ev = function_scope_evaluation_template

    ev.locations.load_spatial(
        in_path=Path(
            TEST_STUDY_DATA_DIR,
            "geo",
            "USGS_PlatteRiver_location.parquet"
        )
    )
    ev.location_crosswalks.load_csv(
        in_path=Path(
            TEST_STUDY_DATA_DIR, "geo", "hefs_usgs_crosswalk.csv"
        )
    )
    # Add USGS observations from test file.
    ev.configurations.add(
        teehr.Configuration(
            name="usgs_climatology",
            timeseries_type="primary",
            description="USGS climatology data"
        )
    )
    ev.variables.add(
        [
            teehr.Variable(
                name="streamflow_hourly_climatology",
                long_name="Climatology of USGS streamflow for hour of year"
            )
        ]
    )
    ev.primary_timeseries.load_parquet(
        in_path=Path(
            TEST_STUDY_DATA_DIR,
            "timeseries",
            "usgs_hefs_06711565_2yr_climatology.parquet"
        ),
        constant_field_values={"unit_name": "ft^3/s"}
    )
    # Add HEFS hindcasts from test file.
    ev.configurations.add(
        teehr.Configuration(
            name="MEFP",
            timeseries_type="secondary",
            description="MBRFC HEFS Data"
        )
    )
    constant_field_values = {
        "unit_name": "ft^3/s",
        "variable_name": "streamflow_hourly_inst",
    }
    ev.secondary_timeseries.load_fews_xml(
        in_path=Path(
            TEST_STUDY_DATA_DIR,
            "timeseries",
            "MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
        ),
        constant_field_values=constant_field_values
    )
    # Calculate a reference forecast, assigning the USGS observation
    # values to an HEFS member (just for testing).
    ref_fcst = bm.ReferenceForecast()

    reference_table_name = "primary_timeseries"
    reference_table_filters = [
        "configuration_name = 'usgs_climatology'",
        "variable_name = 'streamflow_hourly_climatology'",
        "unit_name = 'ft^3/s'"
    ]
    template_table_name = "secondary_timeseries"
    template_table_filters = [
        "configuration_name = 'MEFP'",
        "variable_name = 'streamflow_hourly_inst'",
        "unit_name = 'ft^3/s'",
        "member = '1993'"
    ]
    # If the user has control over the name, they need to add it manually.
    ev.configurations.add(
        teehr.Configuration(
            name="benchmark_forecast_daily_normals",
            timeseries_type="secondary",
            description="Reference forecast based on USGS climatology"
        )
    )
    ev.generate.benchmark_forecast(
        method=ref_fcst,
        reference_table_name=reference_table_name,
        template_table_name=template_table_name,
        reference_table_filters=reference_table_filters,
        template_table_filters=template_table_filters,
        output_configuration_name="benchmark_forecast_daily_normals"
    ).write(destination_table="secondary_timeseries")

    ref_fcst_df = ev.secondary_timeseries.filter(
        "configuration_name = 'benchmark_forecast_daily_normals'"
    ).to_pandas()

    # Values at reference forecast value_times should match USGS climatology.
    usgs_clim_df = ev.primary_timeseries.to_pandas()
    for vt in ref_fcst_df.value_time.unique():
        usgs_clim_value = usgs_clim_df[
            usgs_clim_df.value_time == vt
        ].value.values[0]
        ref_fcst_value = ref_fcst_df[
            ref_fcst_df.value_time == vt
        ].value.values[0]
        assert usgs_clim_value == ref_fcst_value
