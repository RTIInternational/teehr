"""Tests for the domain update columns."""
from teehr.models.pydantic_table_models import (
    Configuration,
    Unit,
    Variable,
    Attribute,
)
import tempfile
from teehr import Evaluation
from pathlib import Path


def test_add_domains(tmpdir):
    """Test creating a new study."""
    # ev = Evaluation()  # Connects to remote by default

    ev = Evaluation(local_warehouse_dir=tmpdir, create_local_dir=True)
    ev.clone_template()

    new_tbl = ev.table(table_name="new_table")

    # Check configurations.add doesn't add columns
    cols = ev.configurations.to_pandas().columns

    ev.configurations.add(
        configuration=[
            Configuration(
                name="conf1",
                type="secondary",
                description="Configuration 1",
            )
        ]
    )

    df = ev.configurations.to_pandas()

    assert list(cols.sort_values()) == list(df.columns.sort_values())
    assert df.name.iloc[0] == "conf1"
    assert df.type.iloc[0] == "secondary"
    assert df.description.iloc[0] == "Configuration 1"

    # Check units.add doesn't add columns
    cols = ev.units.to_pandas().columns

    ev.units.add(
        unit=[
            Unit(
                name="unit1",
                long_name="Unit 1",
            ),
        ]
    )

    new_cols = ev.units.to_pandas().columns

    assert list(cols.sort_values()) == list(new_cols.sort_values())

    # Check variables.add doesn't add columns
    cols = ev.variables.to_pandas().columns

    ev.variables.add(
        variable=[
            Variable(
                name="var1",
                long_name="Variable 1"
            ),
        ]
    )
    new_cols = ev.variables.to_pandas().columns

    assert list(cols.sort_values()) == list(new_cols.sort_values())

    # Check attributes.add doesn't add columns
    cols = ev.attributes.to_pandas().columns

    ev.attributes.add(
        attribute=[
            Attribute(
                name="attr1",
                type="continuous",
                description="Attribute 1",
            ),
        ]
    )
    new_cols = ev.attributes.to_pandas().columns

    assert list(cols.sort_values()) == list(new_cols.sort_values())

    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_add_domains(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )