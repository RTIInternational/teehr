"""Test the import_timeseries function in the Evaluation class."""
from pathlib import Path
from teehr import Evaluation
from teehr.models.tables import (
    Configuration,
    Unit,
    Variable
)
import tempfile
import xarray as xr


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)
TEST_STUDY_DATA_DIR_v0_4 = Path("tests", "data", "test_study")
SUMMA_TIMESERIES_FILEPATH_NC = Path(
    TEST_STUDY_DATA_DIR_v0_4, "timeseries", "summa.example.nc"
)
SUMMA_LOCATIONS = Path(
    TEST_STUDY_DATA_DIR_v0_4, "geo", "summa_locations.parquet"
)
MIZU_TIMESERIES_FILEPATH_NC = Path(
    TEST_STUDY_DATA_DIR_v0_4, "timeseries", "mizuroute.example.nc"
)
MIZU_LOCATIONS = Path(
    TEST_STUDY_DATA_DIR_v0_4, "geo", "mizu_locations.parquet"
)


def test_validate_and_insert_timeseries(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    eval.configurations.add(
        Configuration(
            name="test_obs",
            type="primary",
            description="Test Observations Data"
        )
    )

    eval.units.add(
        Unit(
            name="cfd",
            long_name="Cubic Feet per Day"
        )
    )

    eval.variables.add(
        Variable(
            name="streamflow",
            long_name="Streamflow"
        )
    )

    eval.primary_timeseries.load_parquet(
        in_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        }
    )

    eval.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )

    eval.configurations.add(
        Configuration(
            name="test_short",
            type="secondary",
            description="Test Forecast Data"
        )
    )

    eval.secondary_timeseries.load_parquet(
        in_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        }
    )

    assert True


def test_validate_and_insert_timeseries_set_const(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    eval.primary_timeseries.load_parquet(
        in_path=PRIMARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "usgs_observations"
        }
    )

    eval.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )

    eval.secondary_timeseries.load_parquet(
        in_path=SECONDARY_TIMESERIES_FILEPATH,
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        constant_field_values={
            "unit_name": "m^3/s",
            "variable_name": "streamflow_hourly_inst",
            "configuration_name": "nwm30_retrospective"
        }
    )

    assert True


def test_validate_and_insert_summa_nc_timeseries(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.locations.load_spatial(in_path=SUMMA_LOCATIONS)

    eval.configurations.add(
        Configuration(
            name="summa",
            type="primary",
            description="Summa Runoff Data"
        )
    )

    eval.variables.add(
        Variable(
            name="runoff",
            long_name="runoff"
        )
    )

    summa_field_mapping = {
        "time": "value_time",
        "averageRoutedRunoff_mean": "value",
        "gru": "location_id"
    }
    summa_constant_field_values = {
        "unit_name": "m^3/s",
        "variable_name": "runoff",
        "configuration_name": "summa",
        "reference_time": None
    }

    eval.primary_timeseries.load_netcdf(
        in_path=SUMMA_TIMESERIES_FILEPATH_NC,
        field_mapping=summa_field_mapping,
        constant_field_values=summa_constant_field_values
    )

    # Compare loaded values with the values in the netcdf file.
    primary_df = eval.primary_timeseries.to_pandas()
    primary_df.set_index("location_id", inplace=True)
    teehr_values = primary_df.loc["170300010101"].value.values

    summa_ds = xr.open_dataset(SUMMA_TIMESERIES_FILEPATH_NC)
    nc_values = summa_ds[
        "averageRoutedRunoff_mean"
    ].sel(gru=170300010101).values

    assert (teehr_values == nc_values).all()


def test_validate_and_insert_mizu_nc_timeseries(tmpdir):
    """Test the validate_locations function."""
    eval = Evaluation(dir_path=tmpdir)

    eval.enable_logging()

    eval.clone_template()

    eval.locations.load_spatial(in_path=MIZU_LOCATIONS)

    eval.configurations.add(
        Configuration(
            name="mizuroute",
            type="primary",
            description="Mizuroute Runoff Data"
        )
    )

    eval.variables.add(
        Variable(
            name="runoff",
            long_name="runoff"
        )
    )

    mizu_field_mapping = {
        "time": "value_time",
        "KWroutedRunoff": "value",
        "reachID": "location_id"
    }
    mizu_constant_field_values = {
        "unit_name": "mm/s",
        "variable_name": "runoff",
        "configuration_name": "mizuroute",
        "reference_time": None
    }

    eval.primary_timeseries.load_netcdf(
        in_path=MIZU_TIMESERIES_FILEPATH_NC,
        field_mapping=mizu_field_mapping,
        constant_field_values=mizu_constant_field_values
    )

    # Compare loaded values with the values in the netcdf file.
    primary_df = eval.primary_timeseries.to_pandas()
    primary_df.set_index("location_id", inplace=True)
    teehr_values = primary_df.loc["77000002"].value.values

    mizu_ds = xr.open_dataset(MIZU_TIMESERIES_FILEPATH_NC)
    nc_values = mizu_ds.where(
        mizu_ds.reachID == 77000002, drop=True
    ).KWroutedRunoff.values.ravel()

    assert (teehr_values == nc_values).all()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_validate_and_insert_timeseries(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_validate_and_insert_timeseries_set_const(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_validate_and_insert_summa_nc_timeseries(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_validate_and_insert_mizu_nc_timeseries(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
