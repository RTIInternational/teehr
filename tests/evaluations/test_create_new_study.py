"""Tests for the TEEHR study creation."""
from pathlib import Path

import pytest
from teehr.models.pydantic_table_models import (
    Attribute
)


@pytest.mark.read_write_evaluation_template
def test_clone_template(read_write_evaluation_template):
    """Test creating a new study."""
    ev = read_write_evaluation_template

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
    assert Path(ev.dir_path, ev.active_catalog.catalog_name, "cache").is_dir()
    assert Path(ev.dir_path, ev.active_catalog.catalog_name, "scripts").is_dir()
    assert Path(ev.dir_path, ev.active_catalog.catalog_name, ".gitignore").is_file()