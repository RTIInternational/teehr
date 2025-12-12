from pathlib import Path
from teehr import Configuration
from teehr.models.filters import TableFilter
from teehr.evaluation.evaluation import Evaluation
from teehr import SignatureTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bm

TEST_STUDY_DATA_DIR_v0_4 = Path(Path.cwd(), "tests", "data", "test_study")


def setup_v0_4_ensemble_study(tmpdir):
    """Create a test evaluation with ensemble forecasts using teehr."""
    usgs_location = Path(
            TEST_STUDY_DATA_DIR_v0_4, "geo", "USGS_PlatteRiver_location.parquet"
        )

    secondary_filename = "MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
    secondary_filepath = Path(
        TEST_STUDY_DATA_DIR_v0_4,
        "timeseries",
        secondary_filename
    )
    primary_filepath = Path(
        TEST_STUDY_DATA_DIR_v0_4,
        "timeseries",
        "usgs_hefs_06711565.parquet"
    )

    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    ev.locations.load_spatial(
        in_path=usgs_location
    )
    ev.location_crosswalks.load_csv(
        in_path=Path(
            TEST_STUDY_DATA_DIR_v0_4, "geo", "hefs_usgs_crosswalk.csv"
        )
    )
    ev.configurations.add(
        Configuration(
            name="MEFP",
            type="secondary",
            description="MBRFC HEFS Data"
        )
    )
    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            type="primary",
            description="USGS observed test data"
        )
    )
    constant_field_values = {
        "unit_name": "ft^3/s",
        "variable_name": "streamflow_hourly_inst",
    }
    ev.secondary_timeseries.load_fews_xml(
        in_path=secondary_filepath,
        constant_field_values=constant_field_values
    )
    ev.primary_timeseries.load_parquet(
        in_path=primary_filepath
    )

    # Calculate annual hourly normals from USGS observations.
    input_ts = TableFilter()
    input_ts.table_name = "primary_timeseries"

    ts_normals = sts.Normals()
    ts_normals.temporal_resolution = "hour_of_year"  # the default
    ts_normals.summary_statistic = "mean"           # the default

    ev.generate.signature_timeseries(
        method=ts_normals,
        input_table_filter=input_ts,
        start_datetime="2024-11-19 12:00:00",
        end_datetime="2024-11-21 13:00:00",
        timestep="1 hour",
        fillna=False
    ).write()

    # Add reference forecast based on climatology.
    ev.configurations.add(
        [
            Configuration(
                name="benchmark_forecast_hourly_normals",
                type="secondary",
                description="Reference forecast based on USGS climatology summarized by hour of year"  # noqa
            )
        ]
    )
    ref_fcst = bm.ReferenceForecast()
    ref_fcst.aggregate_reference_timeseries = True

    reference_ts = TableFilter()
    reference_ts.table_name = "primary_timeseries"
    reference_ts.filters = [
        "variable_name = 'streamflow_hour_of_year_mean'",
        "unit_name = 'ft^3/s'"
    ]

    template_ts = TableFilter()
    template_ts.table_name = "secondary_timeseries"
    template_ts.filters = [
        "variable_name = 'streamflow_hourly_inst'",
        "unit_name = 'ft^3/s'",
        "member = '1993'"
    ]
    ev.generate.benchmark_forecast(
        method=ref_fcst,
        reference_table_filter=reference_ts,
        template_table_filter=template_ts,
        output_configuration_name="benchmark_forecast_hourly_normals"
    ).write(destination_table="secondary_timeseries")

    ev.joined_timeseries.create(execute_scripts=False)

    return ev
