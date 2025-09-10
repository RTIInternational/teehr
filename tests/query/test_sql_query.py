"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
import tempfile
from teehr import Evaluation

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study  # noqa


def test_sql_query(tmpdir):
    """Test Evaluation sql query."""
    ev = setup_v0_3_study(tmpdir)
    sdf = ev.sql("""
        SELECT pt.*, u.*, c.* FROM primary_timeseries pt
        JOIN units u ON pt.unit_name = u.name
        JOIN configurations c ON pt.configuration_name = c.name
        LIMIT 10;
    """)
    sdf_cols = sorted(sdf.columns)
    expected_cols = [
        'configuration_name',
        'description',
        'location_id',
        'long_name',
        'name',
        'name',
        'reference_time',
        'type',
        'unit_name',
        'value',
        'value_time',
        'variable_name'
    ]
    assert sdf_cols == expected_cols


def test_sql_query_on_empty_tables(tmpdir):
    """Test sql query on empty table."""
    ev = Evaluation(dir_path=tmpdir, create_dir=True)
    # Enable logging
    ev.enable_logging()
    # Clone the template
    ev.clone_template()
    sdf = ev.sql("""
        SELECT pt.*, u.*, c.* FROM primary_timeseries pt
        JOIN units u ON pt.unit_name = u.name
        JOIN configurations c ON pt.configuration_name = c.name
        LIMIT 10;
    """)
    assert sdf.isEmpty()
    sdf = ev.sql("""
        SELECT * FROM primary_timeseries;
    """)
    assert sdf.isEmpty()
    sdf = ev.sql("""
        SELECT * FROM secondary_timeseries;
    """)
    assert sdf.isEmpty()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_sql_query(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_sql_query_on_empty_tables(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
