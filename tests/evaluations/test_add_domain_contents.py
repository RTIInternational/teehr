"""Tests for the domain update columns"""
from teehr.models.pydantic_table_models import (
    Configuration,
    Unit,
    Variable,
    Attribute,
)
import tempfile


def test_add_domains(tmpdir):
    """Test creating a new study."""
    from teehr import Evaluation

    ev = Evaluation(dir_path=tmpdir)
    ev.clone_template()

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

    new_cols = ev.configurations.to_pandas().columns

    assert list(cols.sort_values()) == list(new_cols.sort_values())

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