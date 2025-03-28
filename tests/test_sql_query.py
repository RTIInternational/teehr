"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
import tempfile
import pytest

from setup_v0_3_study import setup_v0_3_study


def test_sql_query(tmpdir):
    """Test filter string."""
    ev = setup_v0_3_study(tmpdir)
    sdf = ev.sql("""
        SELECT pt.*, u.*, c.* FROM primary_timeseries pt
        JOIN units u ON pt.unit_name = u.name
        JOIN configurations c ON pt.configuration_name = c.name
        LIMIT 10;
    """)
    sdf_cols = sdf.columns
    expected_cols = [
        'value_time',
        'value',
        'unit_name',
        'location_id',
        'configuration_name',
        'variable_name',
        'reference_time',
        'name',
        'long_name',
        'name',
        'type',
        'description'
    ]
    assert sdf_cols == expected_cols



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
