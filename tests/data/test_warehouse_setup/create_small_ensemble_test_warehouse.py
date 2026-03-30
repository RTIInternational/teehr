"""A script to recreate the ensemble_test_warehouse_small.tar.gz for testing."""  # noqa
import tarfile
import tempfile
from pathlib import Path

import teehr
from teehr import SignatureTimeseriesGenerators as sts
from teehr import BenchmarkForecastGenerators as bm


def _create_warehouse(dir_path):
    """Create ensemble_test_warehouse_small.tar.gz."""
    ev = teehr.LocalReadWriteEvaluation(
        dir_path=Path(dir_path) / "ensemble_test_warehouse_small",
        create_dir=True
    )

    usgs_location = "tests/data/test_warehouse_data/geo/USGS_PlatteRiver_location.parquet"
    secondary_filepath = "tests/data/test_warehouse_data/timeseries/MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
    primary_filepath = "tests/data/test_warehouse_data/timeseries/usgs_hefs_06711565.parquet"
    location_xwalk_path = "tests/data/test_warehouse_data/geo/hefs_usgs_crosswalk.csv"

    ev.locations.load_spatial(in_path=usgs_location)
    ev.location_crosswalks.load_csv(
        in_path=location_xwalk_path
    )
    ev.configurations.add([
        teehr.Configuration(
            name="MEFP",
            timeseries_type="secondary",
            description="MBRFC HEFS Data"
        ),
        teehr.Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="USGS observed test data"
        )
    ])

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

    ts_normals = sts.Normals()
    ts_normals.temporal_resolution = "hour_of_year"  # the default
    ts_normals.summary_statistic = "mean"           # the default

    ev.generate.signature_timeseries(
        method=ts_normals,
        input_table_name="primary_timeseries",
        start_datetime="2024-11-19 12:00:00",
        end_datetime="2024-11-21 13:00:00",
        timestep="1 hour",
        fillna=False
    ).write()

    # Add reference forecast based on climatology.
    ev.configurations.add(
        [
            teehr.Configuration(
                name="benchmark_forecast_hourly_normals",
                timeseries_type="secondary",
                description="Reference forecast based on USGS climatology summarized by hour of year"  # noqa
            )
        ]
    )
    ref_fcst = bm.ReferenceForecast()
    ref_fcst.aggregate_reference_timeseries = True

    reference_table_name = "primary_timeseries"
    reference_filters = [
        "variable_name = 'streamflow_hour_of_year_mean'",
        "unit_name = 'ft^3/s'"
    ]
    template_table_name = "secondary_timeseries"
    template_filters = [
        "variable_name = 'streamflow_hourly_inst'",
        "unit_name = 'ft^3/s'",
        "member = '1993'"
    ]

    ev.generate.benchmark_forecast(
        method=ref_fcst,
        reference_table_name=reference_table_name,
        reference_table_filters=reference_filters,
        template_table_name=template_table_name,
        template_table_filters=template_filters,
        output_configuration_name="benchmark_forecast_hourly_normals"
    ).write(destination_table="secondary_timeseries")

    ev.joined_timeseries_view().write("joined_timeseries")

    # Save the warehouse to a tar.gz file for testing
    # Note. This will silently overwrite the file if it already exists.
    output = "tests/data/test_warehouse_data/ensemble_test_warehouse_small.tar.gz"
    with tarfile.open(output, "w:gz") as tar:
        tar.add(ev.dir_path, arcname=ev.dir_path.name)


def main():
    """Create the ensemble_test_warehouse_small.tar.gz file."""
    with tempfile.TemporaryDirectory() as tmpdir:
        _create_warehouse(tmpdir)


if __name__ == "__main__":
    main()
