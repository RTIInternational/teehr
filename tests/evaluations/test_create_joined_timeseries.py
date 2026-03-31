"""Test the import_timeseries function in the LocalReadWriteEvaluation class."""
from pathlib import Path
from teehr import Configuration
from teehr.models.pydantic_table_models import Variable, Unit
import geopandas as gpd
import numpy as np
import pytest
import time

TEST_DATA_DIR = Path("tests", "data", "test_warehouse_data")
GEO_DIR_PATH = Path(TEST_DATA_DIR, "geo")

GEOJSON_GAGES_FILEPATH = Path(GEO_DIR_PATH, "gages.geojson")
PRIMARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_obs.parquet"
)
CROSSWALK_FILEPATH = Path(GEO_DIR_PATH, "crosswalk.csv")
SECONDARY_TIMESERIES_FILEPATH = Path(
    TEST_DATA_DIR, "timeseries", "test_short_fcast.parquet"
)


@pytest.mark.function_scope_evaluation_template
def test_create_joined_timeseries(function_scope_evaluation_template):
    """Test the validate_locations function."""
    t0 = time.time()
    ev = function_scope_evaluation_template
    print("LocalReadWriteEvaluation setup time:", time.time() - t0)

    t0 = time.time()
    # Load the location data
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)
    print("Load locations time:", time.time() - t0)

    t0 = time.time()
    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="test primary configuration"
        )
    )
    print("Add configuration time:", time.time() - t0)

    t0 = time.time()
    # Load the timeseries data and map over the fields and set constants
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
    print("Load primary timeseries time:", time.time() - t0)

    t0 = time.time()
    # Load the crosswalk data
    ev.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )
    print("Load location crosswalks time:", time.time() - t0)

    t0 = time.time()
    ev.configurations.add(
        Configuration(
            name="nwm30_retrospective",
            timeseries_type="secondary",
            description="test secondary configuration"
        )
    )
    print("Add secondary configuration time:", time.time() - t0)

    t0 = time.time()
    # Load the secondary timeseries data and map over the fields
    #  and set constants
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
    print("Load secondary timeseries time:", time.time() - t0)

    t0 = time.time()
    # Load the location attribute data
    ev.location_attributes.load_parquet(
        in_path=GEO_DIR_PATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
        update_attrs_table=True
    )
    print("Load location attributes time:", time.time() - t0)

    t0 = time.time()
    # Create the joined timeseries view with only specified attributes
    # include one invalid attribute 'tester' alongside valid attributes
    attr_list = ['drainage_area', 'ecoregion', 'tester']
    joined = ev.joined_timeseries_view(
        add_attrs=True,
        attr_list=attr_list
    )
    print("Create joined timeseries time:", time.time() - t0)

    columns = joined.to_sdf().columns
    expected_columns = [
        'reference_time',
        'value_time',
        'primary_location_id',
        'secondary_location_id',
        'primary_value',
        'secondary_value',
        'unit_name',
        'drainage_area',
        'ecoregion',
        # 'month',
        # 'year',
        # 'water_year',
        'configuration_name',
        'variable_name',
        'member',
        # 'season'
    ]
    # Make sure secondary geodataframe is created correctly
    secondary_gdf = ev.secondary_timeseries.to_geopandas()

    assert isinstance(secondary_gdf, gpd.GeoDataFrame)
    assert all(np.sort(secondary_gdf["location_id"].unique()) == [
        "fcst-1",
        "fcst-2",
        "fcst-3"
    ])
    assert len(columns) == len(expected_columns)
    assert sorted(columns) == sorted(expected_columns)


@pytest.mark.function_scope_evaluation_template
def test_create_filtered_joined_timeseries(function_scope_evaluation_template):
    """Test the validate_locations function."""
    ev = function_scope_evaluation_template

    # Load the location data
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="test primary configuration"
        )
    )

    # Load the timeseries data and map over the fields and set constants
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

    # Load the crosswalk data
    ev.location_crosswalks.load_csv(
        in_path=CROSSWALK_FILEPATH
    )

    ev.configurations.add(
        Configuration(
            name="nwm30_retrospective",
            timeseries_type="secondary",
            description="test secondary configuration"
        )
    )

    # Load the secondary timeseries data and map over the fields
    #  and set constants
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

    # Load the location attribute data
    ev.location_attributes.load_parquet(
        in_path=GEO_DIR_PATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
        update_attrs_table=True
    )

    # Create the joined timeseries view with only specified attributes
    # include one invalid attribute 'tester' alongside valid attributes
    attr_list = ['drainage_area', 'ecoregion', 'tester']
    joined = ev.joined_timeseries_view(
        add_attrs=True,
        attr_list=attr_list,
        secondary_filters=["location_id = 'fcst-1'"]
    )

    columns = joined.to_sdf().columns
    expected_columns = [
        'reference_time',
        'value_time',
        'primary_location_id',
        'secondary_location_id',
        'primary_value',
        'secondary_value',
        'unit_name',
        'drainage_area',
        'ecoregion',
        # 'month',
        # 'year',
        # 'water_year',
        'configuration_name',
        'variable_name',
        'member',
        # 'season'
    ]

    assert len(columns) == len(expected_columns)
    assert sorted(columns) == sorted(expected_columns)

    joined_df = joined.to_pandas()
    assert all(joined_df['secondary_location_id'].unique() == ['fcst-1'])


@pytest.mark.function_scope_test_warehouse
def test_distinct_values(function_scope_test_warehouse):
    """Test base_table.distinct_values() using joined_timeseries."""
    ev = function_scope_test_warehouse

    # test primary_timeseries with location_prefixes==False (valid)
    distinct_vals = ev.primary_timeseries.distinct_values(column='location_id')
    assert len(distinct_vals) == 3

    # test primary_timeseries with location_prefixes==True (valid)
    prefixes = ev.primary_timeseries.distinct_values(
        column='location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test secondary_timeseries with location_prefixes==True (valid)
    prefixes = ev.secondary_timeseries.distinct_values(
        column='location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "fcst"

    # test joined_timeseries for primary_location_id with
    # location_prefixes==True (valid)
    prefixes = ev.table("joined_timeseries").distinct_values(
        column='primary_location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test joined_timeseries for secondary_location_id with
    # location_prefixes==True (valid)
    prefixes = ev.table("joined_timeseries").distinct_values(
        column='secondary_location_id',
        location_prefixes=True
        )
    assert len(prefixes) == 1
    assert prefixes[0] == "fcst"

    # test locations for id with location_prefixes==True (valid)
    prefixes = ev.locations.distinct_values(
        column='id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test location_attributes for location_id with
    # location_prefixes==True (valid)
    prefixes = ev.location_attributes.distinct_values(
        column='location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test location_crosswalk for primary_location_id with
    # location_prefixes==True (valid)
    prefixes = ev.location_crosswalks.distinct_values(
        column='primary_location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "gage"

    # test location_crosswalk for secondary_location_id with
    # location_prefixes=True (valid)
    prefixes = ev.location_crosswalks.distinct_values(
        column='secondary_location_id',
        location_prefixes=True
    )
    assert len(prefixes) == 1
    assert prefixes[0] == "fcst"

    # test invalid column handling for location_prefixes==True
    with pytest.raises(ValueError):
        prefixes = ev.table("joined_timeseries").distinct_values(
            column='location_id',
            location_prefixes=True
        )

    # test invalid column handling for location_prefixes==False
    with pytest.raises(ValueError):
        prefixes = ev.table("joined_timeseries").distinct_values(column='test')


@pytest.mark.function_scope_evaluation_template
def test_inst_join_across_periods(function_scope_evaluation_template):
    """Test that inst statistic joins across different periods.

    When both primary and secondary have 'inst' statistic, they should join
    regardless of the period (hourly vs daily).
    """
    ev = function_scope_evaluation_template

    # Load the location data
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    # Add variables with inst statistic but different periods
    ev.variables.add(
        Variable(
            name="streamflow_hourly_inst",
            long_name="Hourly Inst Streamflow"
        )
    )
    ev.variables.add(
        Variable(
            name="streamflow_daily_inst",
            long_name="Daily Inst Streamflow"
        )
    )

    ev.units.add(
        Unit(name="m^3/s", long_name="Cubic meters per second")
    )

    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="test primary configuration"
        )
    )

    ev.configurations.add(
        Configuration(
            name="nwm30_retrospective",
            timeseries_type="secondary",
            description="test secondary configuration"
        )
    )

    # Load primary timeseries with hourly_inst variable
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

    # Load the crosswalk data
    ev.location_crosswalks.load_csv(in_path=CROSSWALK_FILEPATH)

    # Load secondary timeseries with daily_inst variable (different period)
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
            "variable_name": "streamflow_daily_inst",
            "configuration_name": "nwm30_retrospective"
        }
    )

    # Create the joined timeseries view - should join despite different periods
    joined = ev.joined_timeseries_view(add_attrs=False)

    joined_df = joined.to_pandas()

    # Should have rows because inst ignores period during join
    assert len(joined_df) > 0

    # The variable_name in joined table should be from secondary (daily_inst)
    assert all(joined_df['variable_name'] == 'streamflow_daily_inst')


@pytest.mark.function_scope_evaluation_template
def test_non_inst_join_requires_matching_period(
    function_scope_evaluation_template
):
    """Test that non-inst statistic requires matching period.

    When primary has 'mean' statistic with hourly period and secondary has
    'mean' with daily period, they should NOT join.
    """
    ev = function_scope_evaluation_template

    # Load the location data
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    # Add variables with mean statistic but different periods
    ev.variables.add(
        Variable(
            name="streamflow_hourly_mean",
            long_name="Hourly Mean Streamflow"
        )
    )
    ev.variables.add(
        Variable(
            name="streamflow_daily_mean",
            long_name="Daily Mean Streamflow"
        )
    )

    ev.units.add(
        Unit(name="m^3/s", long_name="Cubic meters per second")
    )

    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="test primary configuration"
        )
    )

    ev.configurations.add(
        Configuration(
            name="nwm30_retrospective",
            timeseries_type="secondary",
            description="test secondary configuration"
        )
    )

    # Load primary timeseries with hourly_mean variable
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
            "variable_name": "streamflow_hourly_mean",
            "configuration_name": "usgs_observations"
        }
    )

    # Load the crosswalk data
    ev.location_crosswalks.load_csv(in_path=CROSSWALK_FILEPATH)

    # Load secondary timeseries with daily_mean variable (different period)
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
            "variable_name": "streamflow_daily_mean",
            "configuration_name": "nwm30_retrospective"
        }
    )

    # Create the joined timeseries view - should NOT join due to different periods
    joined = ev.joined_timeseries_view(add_attrs=False)

    joined_df = joined.to_pandas()

    # Should have NO rows because non-inst requires matching period
    assert len(joined_df) == 0, \
        "non-inst statistic should not join across different periods"


@pytest.mark.function_scope_evaluation_template
def test_non_inst_join_with_matching_period(
    function_scope_evaluation_template
):
    """Test that non-inst statistic with matching period does join.

    When both primary and secondary have 'mean' statistic with the same
    period (hourly), they should join.
    """
    ev = function_scope_evaluation_template

    # Load the location data
    ev.locations.load_spatial(in_path=GEOJSON_GAGES_FILEPATH)

    # Add variable with mean statistic and same period for both
    ev.variables.add(
        Variable(
            name="streamflow_hourly_mean",
            long_name="Hourly Mean Streamflow"
        )
    )

    ev.units.add(
        Unit(name="m^3/s", long_name="Cubic meters per second")
    )

    ev.configurations.add(
        Configuration(
            name="usgs_observations",
            timeseries_type="primary",
            description="test primary configuration"
        )
    )

    ev.configurations.add(
        Configuration(
            name="nwm30_retrospective",
            timeseries_type="secondary",
            description="test secondary configuration"
        )
    )

    # Load primary timeseries with hourly_mean variable
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
            "variable_name": "streamflow_hourly_mean",
            "configuration_name": "usgs_observations"
        }
    )

    # Load the crosswalk data
    ev.location_crosswalks.load_csv(in_path=CROSSWALK_FILEPATH)

    # Load secondary timeseries with SAME hourly_mean variable
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
            "variable_name": "streamflow_hourly_mean",
            "configuration_name": "nwm30_retrospective"
        }
    )

    # Create the joined timeseries view - should join
    joined = ev.joined_timeseries_view(add_attrs=False)

    joined_df = joined.to_pandas()

    # Should have rows because non-inst with matching period should join
    assert len(joined_df) > 0, \
        "non-inst statistic with matching period should join"

    # The variable_name in joined table should be hourly_mean
    assert all(joined_df['variable_name'] == 'streamflow_hourly_mean')
