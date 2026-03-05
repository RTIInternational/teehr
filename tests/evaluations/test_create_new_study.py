"""Tests for the TEEHR study creation."""
from pathlib import Path

import pytest
from teehr.models.pydantic_table_models import (
    Attribute
)
import teehr


@pytest.mark.function_scope_evaluation_template
def test_list_tables_and_views(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

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

    sdf = ev.attributes.to_sdf()
    sdf.createOrReplaceTempView("attributes")
    views_df = ev.list_views()

    # Not a complete test, but at least we know the function runs.
    assert len(tbls_df) == 9
    assert len(views_df) == 1
    assert Path(ev.dir_path, ev.active_catalog.catalog_name, "cache").is_dir()


@pytest.mark.spark_shared_session
def test_evaluation_initialization(spark_shared_session, tmpdir):
    """Test creating a new study."""
    ev = teehr.Evaluation(
        dir_path=tmpdir,
        spark=spark_shared_session,
        create_dir=True
    )

    pass