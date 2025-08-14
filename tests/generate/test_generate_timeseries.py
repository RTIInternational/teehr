"""Testing utilities for generating synthetic time series data."""
from pathlib import Path
import tempfile

import teehr

TEST_STUDY_DATA_DIR_v0_4 = Path("tests", "data", "test_study")


def test_climatology(tmpdir):
    """Test the climatology calculation."""
    usgs_location = Path(
        TEST_STUDY_DATA_DIR_v0_4, "geo", "USGS_PlatteRiver_location.parquet"
    )
    ev = teehr.Evaluation(dir_path=tmpdir)
    ev.clone_template()
    ev.locations.load_spatial(
        in_path=usgs_location
    )
    # Add USGS observations from test file.
    ev.configurations.add(
        [
            teehr.Configuration(
                name="usgs_observations",
                type="primary",
                description="USGS streamflow observations"
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
    # Calculate climatology from USGS observations.
    ev.configurations.add(
        [
            teehr.Configuration(
                name="usgs_climatology",
                type="primary",
                description="Climatology of USGS streamflow"
            )
        ]
    )
    ev.variables.add(
        [
            teehr.Variable(
                name="streamflow_hourly_climatology",
                long_name="Climatology of USGS streamflow for hour of year"
            )
        ]
    )
    ev.generate.climatology(
        input_timeseries_filter=[
            "configuration_name = 'usgs_observations'",
            "variable_name = 'streamflow_hourly_inst'"
        ],
        output_configuration_name="usgs_climatology",
        output_variable_name="streamflow_hourly_climatology",
        temporal_resolution="day_of_year",
        summary_statistic="mean",
    )
    prim_df = (
        ev
        .primary_timeseries
        .filter("configuration_name = 'usgs_observations'")
        .to_pandas()
    )
    clim_df = (
        ev
        .primary_timeseries
        .filter("configuration_name = 'usgs_climatology'")
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
    # After leap day of year, indices are shifted by one.
    clim_df["day_of_year"] = clim_df.value_time.dt.dayofyear
    assert clim_df[clim_df.day_of_year == 59].value.values[0] == mean_prim_srs.loc[59]
    assert clim_df[clim_df.day_of_year == 61].value.values[0] == mean_prim_srs.loc[60]


def test_reference_forecast(tmpdir):
    """Test the reference forecast calculation."""
    ev = teehr.Evaluation(dir_path=tmpdir)
    ev.clone_template()
    ev.locations.load_spatial(
        in_path=Path(TEST_STUDY_DATA_DIR_v0_4, "geo", "USGS_PlatteRiver_location.parquet")
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
    ev.configurations.add(
        teehr.Configuration(
            name="reference_climatology_forecast",
            type="secondary",
            description="Reference forecast based on USGS climatology"
        )
    )
    ev.secondary_timeseries.create_reference_forecast(
        reference_timeseries_filter=[
            "configuration_name = 'usgs_climatology'",
        ],
        template_forecast_filter=[
            "configuration_name = 'MEFP'",
            "variable_name = 'streamflow_hourly_inst'"
        ],
        output_configuration_name="reference_climatology_forecast",
        method="climatology",
        temporal_resolution="day_of_year",
        aggregate_reference_timeseries=False
    )
    ref_fcst_df = ev.secondary_timeseries.filter(
        "configuration_name = 'reference_climatology_forecast'"
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
        test_climatology(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_reference_forecast(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
