"""Test the import_timeseries function in the LocalReadWriteEvaluation class."""
from pathlib import Path

import pytest
from teehr import DeterministicMetrics as m
from teehr.models.pydantic_table_models import (
    Configuration,
    Unit,
    Variable
)
import xarray as xr
import pandas as pd
import numpy as np
import geopandas as gpd

import time


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_warehouse_data")
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
SUMMA_TIMESERIES_FILEPATH_NC = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "summa.example.nc"
)
SUMMA_LOCATIONS = Path(
    TEST_STUDY_DATA_DIR, "geo", "summa_locations.parquet"
)
MIZU_TIMESERIES_FILEPATH_NC = Path(
    TEST_STUDY_DATA_DIR, "timeseries", "mizuroute.example.nc"
)
MIZU_LOCATIONS = Path(
    TEST_STUDY_DATA_DIR, "geo", "mizu_locations.parquet"
)


@pytest.mark.function_scope_evaluation_template
def test_load_spark_dataframe(function_scope_evaluation_template):
    """Test the load_dataframe function."""
    ev = function_scope_evaluation_template
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
    # Create a pyspark dataframe to load.
    data = {
        "reference_time": ["2024-02-15T08:00:00Z"],
        "value_time": ["2024-02-15T08:00:00Z"],
        "configuration_name": ["test_obs"],
        "unit_name": ["cfd"],
        "variable_name": ["streamflow"],
        "value": [1.9],
        "location_id": ["gage-A"]
    }
    rows = [dict(zip(data, t)) for t in zip(*data.values())]
    df = ev.spark.createDataFrame(rows)
    ev.primary_timeseries.load_dataframe(
        df=df
    )
    # Verify that the data was loaded correctly
    loaded_df = ev.primary_timeseries.to_pandas()
    assert loaded_df.index.size == 1
    loaded_row = loaded_df.iloc[0]
    assert loaded_row["configuration_name"] == "test_obs"
    assert loaded_row["unit_name"] == "cfd"
    assert loaded_row["variable_name"] == "streamflow"
    assert loaded_row["location_id"] == "gage-A"
    assert loaded_row["value"] == 1.9


@pytest.mark.function_scope_evaluation_template
def test_dropping_duplicates(function_scope_evaluation_template):
    """Test the dropping duplicates function."""
    ev = function_scope_evaluation_template

    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    t0 = time.time()
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
    print("Domain variables added in %.2f seconds." % (time.time() - t0))

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

    # sdf = ev.primary_timeseries.to_sdf()
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
    # assert sdf.count() == 78
    assert df.index.size == 78
    assert df.drop_duplicates(
        subset=ev.primary_timeseries.uniqueness_fields
    ).index.size == 78


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_timeseries(function_scope_evaluation_template):
    """Test the validate_locations function."""
    ev = function_scope_evaluation_template

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


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_timeseries_set_const(function_scope_evaluation_template):
    """Test the validate_locations function."""
    ev = function_scope_evaluation_template

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


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_summa_nc_timeseries(function_scope_evaluation_template):
    """Test the validate_locations function."""
    ev = function_scope_evaluation_template

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


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_mizu_nc_timeseries(function_scope_evaluation_template):
    """Test the validate_locations function."""
    ev = function_scope_evaluation_template

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


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_fews_xml_timeseries(function_scope_evaluation_template):
    """Test the validate_locations function."""
    usgs_location = Path(
        TEST_STUDY_DATA_DIR, "geo", "USGS_PlatteRiver_location.parquet"
    )
    secondary_filepath = Path(
        TEST_STUDY_DATA_DIR,
        "timeseries",
        "MEFP.MBRFC.DNVC2LOCAL.SQIN.xml"
    )
    primary_filepath = Path(
        TEST_STUDY_DATA_DIR,
        "timeseries",
        "usgs_hefs_06711565.parquet"
    )

    ev = function_scope_evaluation_template

    ev.locations.load_spatial(
        in_path=usgs_location
    )
    ev.location_crosswalks.load_csv(
        in_path=Path(
            TEST_STUDY_DATA_DIR, "geo", "hefs_usgs_crosswalk.csv"
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
    ev.joined_timeseries_view().write("joined_timeseries")

    # Now, metrics.
    kge = m.KlingGuptaEfficiency()
    include_metrics = [kge]

    metrics_df = ev.metrics.query(
        include_metrics=include_metrics,
        group_by=["primary_location_id", "reference_time"],
        order_by=["primary_location_id"],
    ).to_geopandas()

    assert metrics_df.shape == (1, 4)
    assert metrics_df["primary_location_id"].nunique() == 1


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_in_memory_timeseries(function_scope_evaluation_template):
    """Test the validate_locations function."""
    t0 = time.time()
    ev = function_scope_evaluation_template
    print("LocalReadWriteEvaluation template loaded in %.2f seconds." % (time.time() - t0))

    t0 = time.time()
    gdf = gpd.read_file(GEOJSON_GAGES_FILEPATH)
    ev.locations.load_dataframe(df=gdf)
    print("Locations loaded in %.2f seconds." % (time.time() - t0))

    t0 = time.time()
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
    print("Domain variables added in %.2f seconds." % (time.time() - t0))  # ~ 40 secs!

    t0 = time.time()
    df = pd.read_parquet(PRIMARY_TIMESERIES_FILEPATH)
    ev.primary_timeseries.load_dataframe(
        df=df,
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
    assert len(df) == len(ev.primary_timeseries.to_pandas())
    print("Primary timeseries loaded in %.2f seconds." % (time.time() - t0))

    t0 = time.time()
    df = pd.read_csv(CROSSWALK_FILEPATH)
    ev.location_crosswalks.load_dataframe(df=df)
    print("Location crosswalks loaded in %.2f seconds." % (time.time() - t0))

    t0 = time.time()
    df = pd.read_parquet(SECONDARY_TIMESERIES_FILEPATH)
    ev.secondary_timeseries.load_dataframe(
        df=df,
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
    print("Secondary timeseries loaded in %.2f seconds." % (time.time() - t0))
    assert len(df) == len(ev.secondary_timeseries.to_pandas())