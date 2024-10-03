"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
import tempfile
import pytest

from setup_v0_3_study import setup_v0_3_study


def test_chain_filter_single_str(tmpdir):
    """Test filter string."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.filter("location_id = 'gage-A'").to_pandas()
    assert len(df) == 26


def test_chain_filter_single_str2(tmpdir):
    """Test filter string with invalid id."""
    eval = setup_v0_3_study(tmpdir)
    with pytest.raises(Exception):
        eval.primary_timeseries.filter("id = 'gage-A'").to_pandas()


def test_chain_filter_single_dict(tmpdir):
    """Test filter dict."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.filter({
        "column": "location_id",
        "operator": "=",
        "value": "gage-A"
    }).to_pandas()
    assert len(df) == 26


def test_chain_filter_single_dict2(tmpdir):
    """Test filter dict with invalid id."""
    eval = setup_v0_3_study(tmpdir)
    with pytest.raises(Exception):
        eval.primary_timeseries.filter({
            "column": "id",
            "operator": "=",
            "value": "gage-A"
        }).to_pandas()


def test_chain_filter_single_model(tmpdir):
    """Test filter model."""
    from teehr.models.filters import (
        TimeseriesFilter,
        FilterOperators
    )
    eval = setup_v0_3_study(tmpdir)
    flds = eval.primary_timeseries.field_enum()
    df = eval.primary_timeseries.filter(
        TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


def test_chain_filter_single_model2(tmpdir):
    """Test filter model."""
    from teehr.models.filters import (
        TimeseriesFilter,
        FilterOperators
    )
    eval = setup_v0_3_study(tmpdir)
    flds = eval.primary_timeseries.field_enum()
    with pytest.raises(Exception):
        eval.primary_timeseries.filter(
            TimeseriesFilter(
                column=flds.id,
                operator=FilterOperators.eq,
                value="gage-A"
            )
        ).to_pandas()


def test_chain_filter_list_str(tmpdir):
    """Test filter list of strings."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.filter([
        "location_id = 'gage-A'",
        "value_time > '2022-01-01T12:00:00'"
    ]).to_pandas()
    assert len(df) == 13


def test_chain_filter_list_dict(tmpdir):
    """Test filter list of dicts."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.filter([
        {
            "column": "location_id",
            "operator": "=",
            "value": "gage-A"
        },
        {
            "column": "value_time",
            "operator": ">",
            "value": "2022-01-01T12:00:00Z"
        }
    ]).to_pandas()
    assert len(df) == 13


def test_chain_filter_list_model(tmpdir):
    """Test filter list of models."""
    from teehr.models.filters import (
        TimeseriesFilter,
        FilterOperators
    )
    eval = setup_v0_3_study(tmpdir)
    flds = eval.primary_timeseries.field_enum()
    df = eval.primary_timeseries.filter([
        TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        ),
        TimeseriesFilter(
            column=flds.value_time,
            operator=FilterOperators.gt,
            value="2022-01-01T12:00:00Z"
        )
    ]).to_pandas()
    assert len(df) == 13


def test_query_single_str(tmpdir):
    """Test query string."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.query(
        filters="location_id = 'gage-A'"
    ).to_pandas()
    assert len(df) == 26


def test_query_single_dict(tmpdir):
    """Test query dict."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.query(
        filters={
            "column": "location_id",
            "operator": "=",
            "value": "gage-A"
        }
    ).to_pandas()
    assert len(df) == 26


def test_query_single_model(tmpdir):
    """Test query model."""
    from teehr.models.filters import (
        TimeseriesFilter,
        FilterOperators
    )
    eval = setup_v0_3_study(tmpdir)
    flds = eval.primary_timeseries.field_enum()
    df = eval.primary_timeseries.query(
        filters=TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


def test_query_list_str(tmpdir):
    """Test query list of strings."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.query(
        filters=[
            "location_id = 'gage-A'",
            "value_time > '2022-01-01T12:00:00'"
        ]
    ).to_pandas()
    assert len(df) == 13


def test_query_list_dict(tmpdir):
    """Test query list of dicts."""
    eval = setup_v0_3_study(tmpdir)
    df = eval.primary_timeseries.query(
        filters=[
            {
                "column": "location_id",
                "operator": "=",
                "value": "gage-A"
            },
            {
                "column": "value_time",
                "operator": ">",
                "value": "2022-01-01T12:00:00Z"
            }
        ]
    ).to_pandas()
    assert len(df) == 13


def test_query_list_model(tmpdir):
    """Test query list of models."""
    from teehr.models.filters import (
        TimeseriesFilter,
        FilterOperators
    )
    eval = setup_v0_3_study(tmpdir)
    flds = eval.primary_timeseries.field_enum()
    df = eval.primary_timeseries.query(
        filters=[
            TimeseriesFilter(
                column=flds.location_id,
                operator=FilterOperators.eq,
                value="gage-A"
            ),
            TimeseriesFilter(
                column=flds.value_time,
                operator=FilterOperators.gt,
                value="2022-01-01T12:00:00Z"
            )
        ]
    ).to_pandas()
    assert len(df) == 13


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_chain_filter_single_str(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_chain_filter_single_str2(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_chain_filter_single_dict(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_chain_filter_single_dict2(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
        test_chain_filter_single_model(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
        test_chain_filter_single_model2(
            tempfile.mkdtemp(
                prefix="6-",
                dir=tempdir
            )
        )
        test_chain_filter_list_str(
            tempfile.mkdtemp(
                prefix="7-",
                dir=tempdir
            )
        )
        test_chain_filter_list_dict(
            tempfile.mkdtemp(
                prefix="8-",
                dir=tempdir
            )
        )
        test_chain_filter_list_model(
            tempfile.mkdtemp(
                prefix="9-",
                dir=tempdir
            )
        )
        test_query_single_str(
            tempfile.mkdtemp(
                prefix="10-",
                dir=tempdir
            )
        )
        test_query_single_dict(
            tempfile.mkdtemp(
                prefix="11-",
                dir=tempdir
            )
        )
        test_query_single_model(
            tempfile.mkdtemp(
                prefix="12-",
                dir=tempdir
            )
        )
        test_query_list_str(
            tempfile.mkdtemp(
                prefix="13-",
                dir=tempdir
            )
        )
        test_query_list_dict(
            tempfile.mkdtemp(
                prefix="14-",
                dir=tempdir
            )
        )
        test_query_list_model(
            tempfile.mkdtemp(
                prefix="15-",
                dir=tempdir
            )
        )
