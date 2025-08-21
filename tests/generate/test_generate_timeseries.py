"""Testing utilities for generating synthetic time series data."""
from pathlib import Path
import tempfile

import teehr
from teehr import SignatureTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bm

from teehr.models.generate.base import TimeseriesFilter

TEST_STUDY_DATA_DIR_v0_4 = Path("tests", "data", "test_study")


def test_generate_timeseries_normals(tmpdir):
    """Generate synthetic time series data."""
    ev = teehr.Evaluation(dir_path=tmpdir)
    ev.clone_template()
    usgs_location = Path(
        TEST_STUDY_DATA_DIR_v0_4,
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
                type="primary",
                description="USGS streamflow observations"
            )
        ]
    )
    ev.configurations.add(
        [
            teehr.Configuration(
                name="nwm30_medium_range_forcing",
                type="primary",
                description="Synthetic NWM 30 Medium Range AnA Forcing"
            )
        ]
    )
    ev.primary_timeseries.load_parquet(
        in_path=Path(
            TEST_STUDY_DATA_DIR_v0_4,
            "timeseries",
            "usgs_hefs_06711565_2yrs.parquet"
        )
    )
    ev.primary_timeseries.load_parquet(
        in_path=Path(
            TEST_STUDY_DATA_DIR_v0_4,
            "timeseries",
            "synthetic_nwm_forcing_obs_2yrs.parquet"
        )
    )

    input_ts = TimeseriesFilter()
    input_ts.table_name = "primary_timeseries"
    # input_tsm.unit_name = "m^3/s"  # ft^3/s is the default
    # input_ts.unit_name = None

    ts_normals = sts.Normals()
    ts_normals.temporal_resolution = "day_of_year"  # the default
    ts_normals.summary_statistic = "mean"           # the default

    ev.generate.signature_timeseries(
        method=ts_normals,
        input_timeseries=input_ts,
        start_datetime="2023-01-01T00:00:00",
        end_datetime="2024-12-31T00:00:00",
        timestep="1 hour"
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
    following_leap_day_mask = (prim_df.year == 2024) & (prim_df.day_of_year >= 61)
    prim_df.loc[leap_day_mask, "day_of_year"] = 59
    prim_df.loc[following_leap_day_mask, "day_of_year"] -= 1
    mean_prim_srs = prim_df.copy().groupby("day_of_year")["value"].mean()
    # Check that the climatology matches the manual calculation.
    clim_df["day_of_year"] = clim_df.value_time.dt.dayofyear
    assert clim_df[clim_df.day_of_year == 59].value.values[0] == mean_prim_srs.loc[59]
    assert clim_df[clim_df.day_of_year == 61].value.values[0] == mean_prim_srs.loc[60]


def test_generate_reference_forecast(tmpdir):
    """Test the reference forecast calculation."""
    ev = teehr.Evaluation(dir_path=tmpdir)
    ev.clone_template()
    ev.locations.load_spatial(
        in_path=Path(
            TEST_STUDY_DATA_DIR_v0_4,
            "geo",
            "USGS_PlatteRiver_location.parquet"
        )
    )
    ev.location_crosswalks.load_csv(
        in_path=Path(TEST_STUDY_DATA_DIR_v0_4, "geo", "hefs_usgs_crosswalk.csv")
    )
    # Add USGS observations from test file.
    ev.configurations.add(
        teehr.Configuration(
            name="usgs_climatology",
            type="primary",
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
            TEST_STUDY_DATA_DIR_v0_4,
            "timeseries",
            "usgs_hefs_06711565_2yr_climatology.parquet"
        ),
        constant_field_values={"unit_name": "ft^3/s"}
    )
    # Add HEFS hindcasts from test file.
    ev.configurations.add(
        teehr.Configuration(
            name="MEFP",
            type="secondary",
            description="MBRFC HEFS Data"
        )
    )
    constant_field_values = {
        "unit_name": "ft^3/s",
        "variable_name": "streamflow_hourly_inst",
    }
    ev.secondary_timeseries.load_fews_xml(
        in_path=Path(
            TEST_STUDY_DATA_DIR_v0_4,
            "timeseries",
            "MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
        ),
        constant_field_values=constant_field_values
    )
    # Calculate a reference forecast, assigning the USGS observation
    # values to an HEFS member (just for testing).
    ref_fcst = bm.ReferenceForecast()

    reference_ts = TimeseriesFilter(
        configuration_name="usgs_climatology",
        variable_name="streamflow_hourly_climatology",
        unit_name="ft^3/s",
        table_name="primary_timeseries"
    )

    template_ts = TimeseriesFilter(
        configuration_name="MEFP",
        variable_name="streamflow_hourly_inst",
        unit_name="ft^3/s",
        table_name="secondary_timeseries",
        member="1993"
    )
    # If the user has control over the name, they need to add it manually.
    ev.configurations.add(
        teehr.Configuration(
            name="benchmark_forecast_daily_normals",
            type="secondary",
            description="Reference forecast based on USGS climatology"
        )
    )
    ev.generate.benchmark_forecast(
        method=ref_fcst,
        reference_timeseries=reference_ts,
        template_timeseries=template_ts,
        output_configuration_name="benchmark_forecast_daily_normals"
    ).write(destination_table="secondary_timeseries")

    ref_fcst_df = ev.secondary_timeseries.filter(
        "configuration_name = 'benchmark_forecast_daily_normals'"
    ).to_pandas()

    # Values at reference forecast value_times should match USGS climatology.
    usgs_clim_df = ev.primary_timeseries.to_pandas()
    for vt in ref_fcst_df.value_time.unique():
        usgs_clim_value = usgs_clim_df[usgs_clim_df.value_time == vt].value.values[0]
        ref_fcst_value = ref_fcst_df[ref_fcst_df.value_time == vt].value.values[0]
        assert usgs_clim_value == ref_fcst_value


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_generate_timeseries_normals(
            tempfile.mkdtemp(
                prefix="0-",
                dir=tempdir
            )
        )
        test_generate_reference_forecast(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
