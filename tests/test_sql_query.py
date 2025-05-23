"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
import tempfile

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
