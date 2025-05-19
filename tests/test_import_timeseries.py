"""Test the import_timeseries function in the Evaluation class."""
from pathlib import Path
from teehr import Evaluation
from teehr import DeterministicMetrics as m
from teehr.models.pydantic_table_models import (
    Configuration,
    Unit,
    Variable
)
import tempfile
import xarray as xr
import pandas as pd
import numpy as np


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
PRIMARY_TIMESERIES_DUPS_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "test_short_obs_w_dups.parquet"
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


def test_dropping_duplicates(tmpdir):
    """Test the dropping duplicates function."""
    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)
    ev.configurations.add(
        Configuration(
            name="test_obs",
            type="primary",
            description="Test Observations Data"
        )
    )
    ev.units.add(
        Unit(
            name="cfd",
            long_name="Cubic Feet per Day"
        )
    )
    ev.variables.add(
        Variable(
            name="streamflow",
            long_name="Streamflow"
        )
    )
    # Load the timeseries data
    ev.primary_timeseries.load_parquet(
        in_path=PRIMARY_TIMESERIES_DUPS_FILEPATH,
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
    df = ev.primary_timeseries.to_pandas()
    dups_df = pd.read_parquet(PRIMARY_TIMESERIES_DUPS_FILEPATH)

    assert dups_df.index.size == 156
    assert dups_df.drop_duplicates(
        subset=[
            "location_id",
            "value_time",
            "reference_time",
            "configuration",
            "measurement_unit",
            "variable_name"
        ]
    ).index.size == 78
    assert df.index.size == 78
    assert df.drop_duplicates(
        subset=ev.primary_timeseries.unique_column_set
    ).index.size == 78


def test_validate_and_insert_timeseries(tmpdir):
    """Test the validate_locations function."""
    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    ev.configurations.add(
        Configuration(
            name="test_obs",
            type="primary",
            description="Test Observations Data"
        )
    )
    ev.units.add(
        Unit(
            name="cfd",
            long_name="Cubic Feet per Day"
        )
    )
    ev.variables.add(
        Variable(
            name="streamflow",
            long_name="Streamflow"
        )
    )
    ev.primary_timeseries.load_parquet(
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
    # Test upserting timeseries
    ev.primary_timeseries.load_parquet(
        in_path=Path(
            TEST_STUDY_DATA_DIR,
            "timeseries",
            "test_short_obs_upsert.parquet"
        ),
        field_mapping={
            "reference_time": "reference_time",
            "value_time": "value_time",
            "configuration": "configuration_name",
            "measurement_unit": "unit_name",
            "variable_name": "variable_name",
            "value": "value",
            "location_id": "location_id"
        },
        write_mode="upsert"
    )
    ev.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )
    ev.configurations.add(
        Configuration(
            name="test_short",
            type="secondary",
            description="Test Forecast Data"
        )
    )
    ev.secondary_timeseries.load_parquet(
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
    prim_df = ev.primary_timeseries.to_pandas()
    assert prim_df.value_time.min() == pd.Timestamp("2022-01-01 00:00:00")
    assert prim_df.value_time.max() == pd.Timestamp("2022-01-02 13:00:00")
    assert prim_df.index.size == 114


def test_validate_and_insert_timeseries_set_const(tmpdir):
    """Test the validate_locations function."""
    ev = Evaluation(dir_path=tmpdir)

    ev.enable_logging()

    ev.clone_template()

    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    ev.configurations.add(
        [
            Configuration(
                name="usgs_observations",
                type="primary",
                description="USGS Data"
            ),
            Configuration(
                name="nwm30_retrospective",
                type="secondary",
                description="NWM Data"
            )
        ]
    )

    ev.primary_timeseries.load_parquet(
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

    ev.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )

    ev.secondary_timeseries.load_parquet(
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
    ev = Evaluation(dir_path=tmpdir)

    ev.enable_logging()

    ev.clone_template()

    ev.locations.load_spatial(in_path=SUMMA_LOCATIONS)

    ev.configurations.add(
        Configuration(
            name="summa",
            type="primary",
            description="Summa Runoff Data"
        )
    )

    ev.variables.add(
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

    ev.primary_timeseries.load_netcdf(
        in_path=SUMMA_TIMESERIES_FILEPATH_NC,
        field_mapping=summa_field_mapping,
        constant_field_values=summa_constant_field_values
    )

    # Compare loaded values with the values in the netcdf file.
    primary_df = ev.primary_timeseries.to_pandas()
    primary_df.set_index("location_id", inplace=True)
    teehr_values = primary_df.loc["170300010101"].value.values

    summa_ds = xr.open_dataset(SUMMA_TIMESERIES_FILEPATH_NC)
    nc_values = summa_ds[
        "averageRoutedRunoff_mean"
    ].sel(gru=170300010101).values

    assert (np.sort(teehr_values) == np.sort(nc_values)).all()


def test_validate_and_insert_mizu_nc_timeseries(tmpdir):
    """Test the validate_locations function."""
    ev = Evaluation(dir_path=tmpdir)

    ev.enable_logging()

    ev.clone_template()

    ev.locations.load_spatial(in_path=MIZU_LOCATIONS)

    ev.configurations.add(
        Configuration(
            name="mizuroute",
            type="primary",
            description="Mizuroute Runoff Data"
        )
    )

    ev.variables.add(
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

    ev.primary_timeseries.load_netcdf(
        in_path=MIZU_TIMESERIES_FILEPATH_NC,
        field_mapping=mizu_field_mapping,
        constant_field_values=mizu_constant_field_values
    )

    # Compare loaded values with the values in the netcdf file.
    primary_df = ev.primary_timeseries.to_pandas()
    primary_df.set_index("location_id", inplace=True)
    teehr_values = primary_df.loc["77000002"].value.values

    mizu_ds = xr.open_dataset(MIZU_TIMESERIES_FILEPATH_NC)
    nc_values = mizu_ds.where(
        mizu_ds.reachID == 77000002, drop=True
    ).KWroutedRunoff.values.ravel()

    assert (np.sort(teehr_values) == np.sort(nc_values)).all()


def test_validate_and_insert_fews_xml_timeseries(tmpdir):
    """Test the validate_locations function."""
    usgs_location = Path(
        TEST_STUDY_DATA_DIR_v0_4, "geo", "USGS_PlatteRiver_location.parquet"
    )
    secondary_filepath = Path(
        TEST_STUDY_DATA_DIR_v0_4,
        "timeseries",
        "MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
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
        [
            Configuration(
                name="MEFP",
                type="secondary",
                description="MBRFC HEFS Data"
            ),
            Configuration(
                name="usgs_observations",
                type="primary",
                description="USGS Data"
            )
        ]
    )
    constant_field_values = {
        "unit_name": "ft^3/s",
        "variable_name": "streamflow_hourly_inst"
    }
    ev.secondary_timeseries.load_fews_xml(
        in_path=secondary_filepath,
        constant_field_values=constant_field_values
    )
    ev.primary_timeseries.load_parquet(
        in_path=primary_filepath
    )
    ev.joined_timeseries.create(execute_scripts=False)

    # Now, metrics.
    kge = m.KlingGuptaEfficiency()
    include_metrics = [kge]

    # Define some filters?

    metrics_df = ev.metrics.query(
        include_metrics=include_metrics,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
    ).to_geopandas()

    assert metrics_df.shape == (1, 4)
    assert metrics_df["primary_location_id"].nunique() == 1


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_dropping_duplicates(
            tempfile.mkdtemp(
                prefix="0-",
                dir=tempdir
            )
        )
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
        test_validate_and_insert_fews_xml_timeseries(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
