"""Tests for the TEEHR study creation."""
from pathlib import Path
import tempfile
from teehr.models.pydantic_table_models import (
    Attribute
)
from teehr.evaluation.spark_session_utils import create_spark_session
SPARK_SESSION = create_spark_session()


def test_clone_template(tmpdir, spark_session):
    """Test creating a new study."""

    from teehr import Evaluation

    spark = spark_session.newSession()
    ev = Evaluation(dir_path=tmpdir, create_dir=True, spark=spark)
    ev.clone_template()

    tbls_df = ev.list_tables()

    # Make sure the empty table warning is not raised.
    ev.attributes.add(
        [
            Attribute(
                name="drainage_area",
                type="continuous",
                description="Drainage area in square kilometers"
            )
        ]
    )

    _ = ev.sql("SELECT * FROM attributes", create_temp_views=["attributes"])
    views_df = ev.list_views()

    # Not a complete test, but at least we know the function runs.
    assert len(tbls_df) == 9
    assert len(views_df) == 1
    assert Path(tmpdir, ev.active_catalog.catalog_name, "cache").is_dir()
    assert Path(tmpdir, ev.active_catalog.catalog_name, "scripts").is_dir()
    assert Path(tmpdir, ev.active_catalog.catalog_name, ".gitignore").is_file()
    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_clone_template(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            ),
            SPARK_SESSION
        )
