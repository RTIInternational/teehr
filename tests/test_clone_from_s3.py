"""Test the clone_from_s3 module."""
import tempfile
import pandas as pd
from pathlib import Path
from teehr import Evaluation

from teehr.loading.s3.clone_from_s3 import list_s3_evaluations


def test_get_s3_evaluations_dataframe():
    """Test get_s3_evaluations as a dataframe."""
    df = list_s3_evaluations()
    assert len(df) == 5
    assert isinstance(df, pd.DataFrame)


def test_get_s3_evaluations_list():
    """Test get_s3_evaluations as a list."""
    l = list_s3_evaluations(format="list")
    assert len(l) == 5
    assert isinstance(l, list)


def test_clone_example_from_s3(tmpdir):
    """Test cloning a fully populated evaluation from s3."""
    ev = Evaluation(tmpdir)
    ev.clone_from_s3("e0_2_location_example")

    assert ev.units.to_sdf().count() == 4
    assert ev.variables.to_sdf().count() == 3
    assert ev.attributes.to_sdf().count() == 26
    assert ev.configurations.to_sdf().count() == 2
    assert ev.locations.to_sdf().count() == 2
    assert ev.location_attributes.to_sdf().count() == 50
    assert ev.location_crosswalks.to_sdf().count() == 2
    assert ev.primary_timeseries.to_sdf().count() == 200350
    assert ev.secondary_timeseries.to_sdf().count() == 210384
    assert ev.joined_timeseries.to_sdf().count() == 200350

    assert Path(ev.scripts_dir, "user_defined_fields.py").is_file()


def test_clone_partial_template_from_s3(tmpdir):
    """Test cloning a partially empty evaluation from s3."""
    ev = Evaluation(tmpdir)
    ev.clone_from_s3("e4_nwm_operational")

    # assert ev.units.to_sdf().count() == 9
    # assert ev.variables.to_sdf().count() == 5
    # assert ev.attributes.to_sdf().count() == 13
    # assert ev.configurations.to_sdf().count() == 1
    assert ev.primary_timeseries.to_sdf().count() == 0
    assert ev.secondary_timeseries.to_sdf().count() == 0
    assert ev.joined_timeseries.to_sdf().count() == 0

    assert Path(ev.scripts_dir, "user_defined_fields.py").is_file()


def test_clone_and_subset_example_from_s3(tmpdir):
    """Test filter string."""
    ev = Evaluation(tmpdir)
    ev.clone_from_s3(
        evaluation_name="e0_2_location_example",
        primary_location_ids=["usgs-14316700"],
        start_date="2001-09-30 20:00",
        end_date="2010-09-29 20:00"
    )

    assert ev.units.to_sdf().count() == 4
    assert ev.variables.to_sdf().count() == 3
    assert ev.attributes.to_sdf().count() == 26
    assert ev.configurations.to_sdf().count() == 2
    assert ev.locations.to_sdf().count() == 1
    assert ev.location_attributes.to_sdf().count() == 25
    assert ev.location_crosswalks.to_sdf().count() == 1
    assert ev.primary_timeseries.to_sdf().count() == 74433
    assert ev.secondary_timeseries.to_sdf().count() == 78865
    assert ev.joined_timeseries.to_sdf().count() == 74433
    assert ev.joined_timeseries.to_pandas().value_time.max() == \
        pd.Timestamp("2010-09-29 20:00:00")
    assert ev.joined_timeseries.to_pandas().value_time.min() == \
        pd.Timestamp("2001-09-30 20:00:00")
    assert Path(ev.scripts_dir, "user_defined_fields.py").is_file()


if __name__ == "__main__":
    test_get_s3_evaluations_dataframe()
    test_get_s3_evaluations_list()

    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_clone_example_from_s3(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_clone_partial_template_from_s3(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_clone_and_subset_example_from_s3(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
