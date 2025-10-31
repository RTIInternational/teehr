"""Tests for Iceberg."""
from pathlib import Path
import tempfile

from teehr import Evaluation


def test_clone_template(tmpdir):
    """Test creating a new study."""
    ev = Evaluation(
        dir_path=tmpdir,
        remote_warehouse_dir=Path(tmpdir) / "warehouse",
        create_dir=True,
    )
    ev.clone_template()

    assert ev.spark.sql("SELECT * FROM attributes").count() == 0
    assert ev.spark.sql("SELECT * FROM locations").count() == 0
    assert ev.spark.sql("SELECT * FROM location_attributes").count() == 0
    assert ev.spark.sql("SELECT * FROM location_crosswalks").count() == 0
    assert ev.spark.sql("SELECT * FROM units").count() == 4
    assert ev.spark.sql("SELECT * FROM variables").count() == 4
    assert ev.spark.sql("SELECT * FROM configurations").count() == 0
    assert ev.spark.sql("SELECT * FROM primary_timeseries").count() == 0
    assert ev.spark.sql("SELECT * FROM secondary_timeseries").count() == 0

    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tmpdir:
        test_clone_template(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tmpdir
            )
        )
