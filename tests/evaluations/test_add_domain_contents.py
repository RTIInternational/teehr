"""Tests for the domain update columns."""
from teehr.models.pydantic_table_models import (
    Configuration,
    Unit,
    Variable,
    Attribute,
)
import pytest
import time


@pytest.mark.function_scope_evaluation_template
def test_add_domains(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

    # Check configurations.add doesn't add columns
    cols = ev.configurations.to_pandas().columns

    t0 = time.time()
    ev.configurations.add(
        configuration=[
            Configuration(
                name="conf1",
                timeseries_type="secondary",
                description="Configuration 1",
            )
        ]
    )
    print("Time to add configuration:", time.time() - t0)

    df = ev.configurations.to_pandas()

    assert list(cols.sort_values()) == list(df.columns.sort_values())
    assert df.name.iloc[0] == "conf1"
    assert df.timeseries_type.iloc[0] == "secondary"
    assert df.description.iloc[0] == "Configuration 1"

    # Check units.add doesn't add columns
    cols = ev.units.to_pandas().columns

    t0 = time.time()
    ev.units.add(
        unit=[
            Unit(
                name="unit1",
                long_name="Unit 1",
            ),
        ]
    )
    print("Time to add unit:", time.time() - t0)

    new_cols = ev.units.to_pandas().columns

    assert list(cols.sort_values()) == list(new_cols.sort_values())

    # Check variables.add doesn't add columns
    cols = ev.variables.to_pandas().columns

    t0 = time.time()
    ev.variables.add(
        variable=[
            Variable(
                name="var1",
                long_name="Variable 1"
            ),
        ]
    )
    print("Time to add variable:", time.time() - t0)

    new_cols = ev.variables.to_pandas().columns

    assert list(cols.sort_values()) == list(new_cols.sort_values())

    # Check attributes.add doesn't add columns
    cols = ev.attributes.to_pandas().columns

    t0 = time.time()
    ev.attributes.add(
        attribute=[
            Attribute(
                name="attr1",
                type="continuous",
                description="Attribute 1",
            ),
        ]
    )
    print("Time to add attribute:", time.time() - t0)

    new_cols = ev.attributes.to_pandas().columns

    assert list(cols.sort_values()) == list(new_cols.sort_values())


@pytest.mark.function_scope_evaluation_template
def test_upsert_domain(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

    ev.configurations.add(
        configuration=[
            Configuration(
                name="conf1",
                timeseries_type="secondary",
                description="Configuration 1",
            )
        ]
    )

    ev.configurations.add(
        configuration=[
            Configuration(
                name="conf1",
                timeseries_type="secondary",
                description="Configuration 1a",
            )
        ]
    )

    assert ev.configurations.to_sdf().count() == 1
    assert ev.configurations.to_pandas().description.iloc[0] == "Configuration 1a"


@pytest.mark.function_scope_evaluation_template
def test_invalid_name_domain(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

    with pytest.raises(ValueError, match="name"):
        ev.configurations.add(
            configuration=[
                Configuration(
                    name="special chars !@#$%^&*() not allowed",
                    timeseries_type="secondary",
                    description="Configuration 1",
                )
            ]
        )


@pytest.mark.function_scope_evaluation_template
def test_invalid_type_domain(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

    with pytest.raises(ValueError, match="type"):
        ev.configurations.add(
            configuration=[
                Configuration(
                    name="Configuration 1",
                    timeseries_type="invalid_type",
                    description="Configuration 1",
                )
            ]
        )
