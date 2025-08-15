"""Testing utilities for generating synthetic time series data."""
from pathlib import Path
import tempfile

import teehr
from teehr import SummaryTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bfs

from teehr.models.generate.base import TimeseriesModel

TEST_STUDY_DATA_DIR_v0_4 = Path("tests", "data", "test_study")


def test_generate_timeseries_normals(tmpdir):
    """Generate synthetic time series data."""
    ev = teehr.Evaluation(dir_path=tmpdir)
    ev.clone_template()
    usgs_location = Path(
        TEST_STUDY_DATA_DIR_v0_4, "geo", "USGS_PlatteRiver_location.parquet"
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
    ev.primary_timeseries.load_parquet(
        in_path=Path(
            TEST_STUDY_DATA_DIR_v0_4,
            "timeseries",
            "usgs_hefs_06711565_2yrs.parquet"
        )
    )

    input_tsm = TimeseriesModel()
    input_tsm.unit_name = "m^3/s"  # ft^3/s is the default

    ts_normals = sts.Normals()
    ts_normals.temporal_resolution = "day_of_year"  # the default
    ts_normals.summary_statistic = "mean"           # the default
    ts_normals.input_tsm = input_tsm

    ev.generate.summary_timeseries(method=ts_normals).write()

    # df = gts.to_pandas()
    prim_df = ev.primary_timeseries.to_pandas()

    pass


def test_generate_reference_forecast(tmpdir):
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
    ref_fcst = bfs.ReferenceForecast()
    ref_fcst.reference_tsm = TimeseriesModel(
        configuration_name="usgs_observations",
        variable_name="streamflow_day_of_year_mean",
        unit_name="ft^3/s",
        timeseries_type="primary"
    )
    ref_fcst.template_tsm = TimeseriesModel(
        configuration_name="MEFP",
        variable_name="streamflow_hourly_inst",
        unit_name="ft^3/s",
        timeseries_type="secondary",
        member="1993"
    )
    ref_fcst.output_tsm = TimeseriesModel(
        configuration_name="benchmark_forecast_normals",
        variable_name="streamflow_hourly_inst",
        unit_name="ft^3/s",
        timeseries_type="secondary"
    )

    ev.generate.benchmark_forecast(
        method=ref_fcst
    ).write()

    pass

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
        # test_generate_timeseries_normals(
        #     tempfile.mkdtemp(
        #         prefix="0-",
        #         dir=tempdir
        #     )
        # )
        test_generate_reference_forecast(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
