"""Tests for the filter formatting functions."""
from datetime import datetime
from teehr.models.filters import (
    TimeseriesFilter,
    FilterOperators
)
from teehr.querying.filter_format import (
    format_filter
)
import pandas as pd
import pytest


@pytest.mark.session_scope_test_warehouse
def test_filter_eq_str(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.eq,
        value="foo"
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name = 'foo'"


@pytest.mark.session_scope_test_warehouse
def test_filter_in_str(session_scope_test_warehouse):
    """Test the format_filter_to_str function with in_str."""
    ev = session_scope_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.isin,
        value=["foo", "bar"]
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"


@pytest.mark.session_scope_test_warehouse
def test_filter_in_str_series(session_scope_test_warehouse):
    """Test the format_filter_to_str function with in_str."""
    ev = session_scope_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.isin,
        value=pd.Series(["foo", "bar"])
    )
    filter_str = format_filter(filter)
    assert filter_str == "variable_name in ('foo','bar')"


@pytest.mark.session_scope_test_warehouse
def test_filter_gt_dt(session_scope_test_warehouse):
    """Test the format_filter_to_str function with gt_dt."""
    ev = session_scope_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.reference_time,
        operator=FilterOperators.gt,
        value=datetime(2021, 1, 1)
    )
    filter_str = format_filter(filter)
    assert filter_str == "reference_time > '2021-01-01 00:00:00'"


@pytest.mark.session_scope_test_warehouse
def test_filter_lt_int(session_scope_test_warehouse):
    """Test the format_filter_to_str function lt_int."""
    ev = session_scope_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperators.lt,
        value=50
    )
    filter_str = format_filter(filter)
    assert filter_str == "value < 50"