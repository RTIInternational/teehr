"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
from datetime import timedelta
import pytest
from teehr import RowLevelCalculatedFields as rcf
from teehr.models.filters import (
    TimeseriesFilter,
    JoinedTimeseriesFilter,
    FilterOperators,
)


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_single_str(session_scope_test_warehouse):
    """Test filter string."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.filter("location_id = 'gage-A'").to_pandas()
    assert len(df) == 26


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_single_str2(session_scope_test_warehouse):
    """Test filter string with invalid id."""
    ev = session_scope_test_warehouse
    with pytest.raises(Exception):
        ev.primary_timeseries.filter("id = 'gage-A'").to_pandas()


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_single_dict(session_scope_test_warehouse):
    """Test filter dict."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.filter({
        "column": "location_id",
        "operator": "=",
        "value": "gage-A"
    }).to_pandas()
    assert len(df) == 26


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_single_dict2(session_scope_test_warehouse):
    """Test filter dict with invalid id."""
    ev = session_scope_test_warehouse
    with pytest.raises(Exception):
        ev.primary_timeseries.filter({
            "column": "id",
            "operator": "=",
            "value": "gage-A"
        }).to_pandas()


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_single_model(session_scope_test_warehouse):
    """Test filter model."""
    ev = session_scope_test_warehouse
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.filter(
        TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_single_model2(session_scope_test_warehouse):
    """Test filter model."""
    ev = session_scope_test_warehouse
    flds = ev.primary_timeseries.field_enum()
    with pytest.raises(Exception):
        ev.primary_timeseries.filter(
            TimeseriesFilter(
                column=flds.id,
                operator=FilterOperators.eq,
                value="gage-A"
            )
        ).to_pandas()


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_list_str(session_scope_test_warehouse):
    """Test filter list of strings."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.filter([
        "location_id = 'gage-A'",
        "value_time > '2022-01-01T12:00:00'"
    ]).to_pandas()
    assert len(df) == 13


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_list_dict(session_scope_test_warehouse):
    """Test filter list of dicts."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.filter([
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


@pytest.mark.session_scope_test_warehouse
def test_chain_filter_list_model(session_scope_test_warehouse):
    """Test filter list of models."""
    ev = session_scope_test_warehouse
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.filter([
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


@pytest.mark.session_scope_test_warehouse
def test_query_single_str(session_scope_test_warehouse):
    """Test query string."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters="location_id = 'gage-A'"
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.session_scope_test_warehouse
def test_query_single_dict(session_scope_test_warehouse):
    """Test query dict."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters={
            "column": "location_id",
            "operator": "=",
            "value": "gage-A"
        }
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.session_scope_test_warehouse
def test_query_single_model(session_scope_test_warehouse):
    """Test query model."""
    ev = session_scope_test_warehouse
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.query(
        filters=TimeseriesFilter(
            column=flds.location_id,
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.session_scope_test_warehouse
def test_query_list_str(session_scope_test_warehouse):
    """Test query list of strings."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters=[
            "location_id = 'gage-A'",
            "value_time > '2022-01-01T12:00:00'"
        ]
    ).to_pandas()
    assert len(df) == 13


@pytest.mark.session_scope_test_warehouse
def test_query_list_dict(session_scope_test_warehouse):
    """Test query list of dicts."""
    ev = session_scope_test_warehouse
    df = ev.primary_timeseries.query(
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


@pytest.mark.session_scope_test_warehouse
def test_query_list_model(session_scope_test_warehouse):
    """Test query list of models."""
    ev = session_scope_test_warehouse
    flds = ev.primary_timeseries.field_enum()
    df = ev.primary_timeseries.query(
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


@pytest.mark.session_scope_test_warehouse
def test_filter_by_lead_time(session_scope_test_warehouse):
    """Test filter by lead time."""
    ev = session_scope_test_warehouse
    ev.joined_timeseries.add_calculated_fields([
        rcf.ForecastLeadTime(),
    ]).write()
    filter_value = timedelta(days=0, hours=18)
    flds = ev.joined_timeseries.field_enum()
    df = ev.joined_timeseries.query(
            JoinedTimeseriesFilter(
                column=flds.forecast_lead_time,
                operator=FilterOperators.gt,
                value=filter_value
            )
    ).to_pandas()
    assert len(df) == 45
    df = ev.joined_timeseries.filter(
        filters=[
            {
                "column": "forecast_lead_time",
                "operator": ">",
                "value": filter_value
            }
        ]
    ).to_pandas()
    assert len(df) == 45

    df = ev.joined_timeseries.filter(
        "forecast_lead_time > interval 18 hours"
    ).to_pandas()
    assert len(df) == 45

    df = ev.joined_timeseries.filter(
        "forecast_lead_time < interval 1 day"
    ).to_pandas()
    assert len(df) == 216

    df = ev.joined_timeseries.filter(
        "forecast_lead_time < interval 3600 seconds"
    ).to_pandas()
    assert len(df) == 9
