import tempfile
import pytest
import pandas as pd
from pathlib import Path

from setup_v0_3_study import setup_v0_3_study

from teehr.fetching.s3.clone_from_s3 import list_s3_evaluations, clone_from_s3

def test_get_s3_evaluations_dataframe():
    """Test get_s3_evaluations as a dataframe."""
    df = list_s3_evaluations()
    assert len(df) == 4
    assert isinstance(df, pd.DataFrame)

def test_get_s3_evaluations_list():
    """Test get_s3_evaluations as a list."""
    l = list_s3_evaluations(format="list")
    assert len(l) == 4
    assert isinstance(l, list)


def test_clone_example_form_s3(tmpdir):
    """Test filter string."""
    ev = setup_v0_3_study(tmpdir)
    clone_from_s3(ev, "p0_2_location_example")

    assert ev.units.to_sdf().count() == 4
    assert ev.variables.to_sdf().count() == 3
    assert ev.attributes.to_sdf().count() == 26
    assert ev.configurations.to_sdf().count() == 161
    assert ev.locations.to_sdf().count() == 2
    assert ev.location_attributes.to_sdf().count() == 50
    assert ev.location_crosswalks.to_sdf().count() == 2
    assert ev.primary_timeseries.to_sdf().count() == 200350
    assert ev.secondary_timeseries.to_sdf().count() == 210384
    assert ev.joined_timeseries.to_sdf().count() == 200350

    assert Path(ev.scripts_dir, "user_defined_fields.py").is_file()


if __name__ == "__main__":
    test_get_s3_evaluations_dataframe()
    test_get_s3_evaluations_list()

    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_clone_example_form_s3(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )