"""Tests for the TEEHR study creation."""
from pathlib import Path

import pytest


@pytest.mark.function_scope_evaluation_template
def test_clone_template(function_scope_evaluation_template):
    """Test creating a new study."""
    ev = function_scope_evaluation_template

    tbls_df = ev.list_tables()

    catalog_name = ev.active_catalog.catalog_name

    # Not a complete test, but at least we know the function runs.
    assert len(tbls_df) == 9
    assert Path(ev.dir_path, catalog_name, "cache").is_dir()
    assert Path(ev.dir_path, catalog_name, ".gitignore").is_file()
