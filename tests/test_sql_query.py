"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
import tempfile
from teehr import Evaluation

from setup_v0_3_study import setup_v0_3_study


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
    ev = Evaluation(dir_path=tmpdir)
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


def test_distinct_location_prefixes(tmpdir):
    """Test distinct location prefixes."""
    ev = setup_v0_3_study(tmpdir)
    # test primary_timeseries
    sdf = ev.distinct_location_prefixes("primary_timeseries")
    cols = sdf.columns
    assert not sdf.isEmpty()
    assert cols == ["primary_location_prefix"]
    # test secondary_timeseries
    sdf = ev.distinct_location_prefixes("secondary_timeseries")
    cols = sdf.columns
    assert not sdf.isEmpty()
    assert cols == ["secondary_location_prefix"]
    # test joined_timeseries
    sdf = ev.distinct_location_prefixes("joined_timeseries")
    cols = sdf.columns
    assert not sdf.isEmpty()
    assert cols == ["primary_location_prefix", "secondary_location_prefix"]


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
        test_distinct_location_prefixes(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
