"""Test the clone_from_s3 module."""
import tempfile
import pandas as pd
from pathlib import Path

import pytest
from teehr import Evaluation
from teehr.evaluation.spark_session_utils import create_spark_session


@pytest.mark.skip(reason="In development")
def test_clone_and_subset_example_from_s3(tmpdir):
    """Test filter string."""
    # Note. Currently requires port-forwarding iceberg-rest service
    spark = create_spark_session(
        app_name="test_clone_and_subset_example_from_s3",
        remote_catalog_uri="http://localhost:8181",
        remote_warehouse_dir="s3://dev-teehr-iceberg-warehouse/"
    )
    ev = Evaluation(tmpdir, create_dir=True, spark=spark)
    ev.clone_from_s3(
        primary_location_ids=["usgs-14316700"],
        start_date="2001-09-30 20:00",
        end_date="2010-09-29 20:00"
    )
    assert ev.units.to_sdf().count() == 4
    assert ev.variables.to_sdf().count() == 4
    assert ev.attributes.to_sdf().count() == 50
    assert ev.configurations.to_sdf().count() == 2
    assert ev.locations.to_sdf().count() == 1
    assert ev.location_attributes.to_sdf().count() == 47
    assert ev.location_crosswalks.to_sdf().count() == 1
    assert ev.primary_timeseries.to_sdf().count() == 74433
    assert ev.secondary_timeseries.to_sdf().count() == 78865
    assert ev.joined_timeseries.to_sdf().count() == 74433
    assert ev.joined_timeseries.to_pandas().value_time.max() == \
        pd.Timestamp("2010-09-29 20:00:00")
    assert ev.joined_timeseries.to_pandas().value_time.min() == \
        pd.Timestamp("2001-09-30 20:00:00")
    assert Path(ev.scripts_dir, "user_defined_fields.py").is_file()

    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_clone_and_subset_example_from_s3(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
