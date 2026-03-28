"""Tests created_at and updated_at columns in domains tables."""
import pytest
import teehr


@pytest.mark.function_scope_test_warehouse
def test_domains_upsert_new(function_scope_test_warehouse):
    """Test creating a new study."""
    ev = function_scope_test_warehouse

    # Units
    ev.units.add([teehr.Unit(name="t/s", long_name="Unit 1")])

    # Check that new unit is added and can be read back with created_at and updated_at columns
    sdf = ev.units.filter("long_name = 'Unit 1'").to_sdf()

    # assert that created_at is not NULL and updated_at is NULL (since it's a new record)
    assert sorted(sdf.columns) == sorted(["name", "long_name", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 1
    assert sdf.filter("updated_at IS NOT NULL").count() == 1

    # Configurations
    ev.configurations.add([teehr.Configuration(name="conf_1", type="primary", description="Configuration 1")])

    # Check that new configuration is added and can be read back with created_at and updated_at columns
    sdf = ev.configurations.filter("name = 'conf_1'").to_sdf()

    # assert that created_at is not NULL and updated_at is NULL (since it's a new record)
    assert sorted(sdf.columns) == sorted(["name", "type", "description", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 1
    assert sdf.filter("updated_at IS NOT NULL").count() == 1

    # Variables
    ev.variables.add([teehr.Variable(name="var_1", long_name="Variable 1")])

    sdf = ev.variables.filter("long_name = 'Variable 1'").to_sdf()

    # assert that created_at is not NULL and updated_at is NULL (since it's a new record)
    assert sorted(sdf.columns) == sorted(["name", "long_name", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 1
    assert sdf.filter("updated_at IS NOT NULL").count() == 1

    # Attributes
    ev.attributes.add([teehr.Attribute(name="attr_1", type="categorical", description="Attribute 1")])

    # Check that new attribute is added and can be read back with created_at and updated_at columns
    sdf = ev.attributes.filter("name = 'attr_1'").to_sdf()

    # assert that created_at is not NULL and updated_at is NULL (since it's a new record)
    assert sorted(sdf.columns) == sorted(["name", "type", "description", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 1
    assert sdf.filter("updated_at IS NOT NULL").count() == 1


@pytest.mark.function_scope_test_warehouse
def test_domains_upsert_existing(function_scope_test_warehouse):
    """Test creating a new study."""
    ev = function_scope_test_warehouse

    # Units
    df = ev.units.filter("name = 'm^3/s'").to_sdf().select("name", "long_name").toPandas()
    ev._write.to_warehouse(df, "units", "upsert")

    sdf = ev.units.filter("name = 'm^3/s'").to_sdf()
    assert sorted(sdf.columns) == sorted(["name", "long_name", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 0
    assert sdf.filter("updated_at IS NOT NULL").count() == 1

    # Configurations
    df = ev.configurations.filter(
        "name = 'usgs_observations'"
    ).to_sdf().select("name", "type", "description").toPandas()
    ev._write.to_warehouse(df, "configurations", "upsert")

    sdf = ev.configurations.filter("name = 'usgs_observations'").to_sdf()
    assert sorted(sdf.columns) == sorted(["name", "type", "description", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 0
    assert sdf.filter("updated_at IS NOT NULL").count() == 1

    # Variables
    df = ev.variables.filter("name = 'streamflow_hourly_inst'").to_sdf().select("name", "long_name").toPandas()
    ev._write.to_warehouse(df, "variables", "upsert")

    sdf = ev.variables.filter("name = 'streamflow_hourly_inst'").to_sdf()
    assert sorted(sdf.columns) == sorted(["name", "long_name", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 0
    assert sdf.filter("updated_at IS NOT NULL").count() == 1

    # Attributes
    df = ev.attributes.filter("name = 'drainage_area'").to_sdf().select("name", "type", "description").toPandas()
    ev._write.to_warehouse(df, "attributes", "upsert")

    sdf = ev.attributes.filter("name = 'drainage_area'").to_sdf()
    assert sorted(sdf.columns) == sorted(["name", "type", "description", "created_at", "updated_at"])
    assert sdf.filter("created_at IS NOT NULL").count() == 0
    assert sdf.filter("updated_at IS NOT NULL").count() == 1
