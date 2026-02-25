"""This module tests the filter validation function."""
from datetime import datetime
from teehr.models.filters import (
    TableFilter,
    FilterOperators
)
import pandas as pd
from teehr.querying.filter_format import validate_filter
import pytest


@pytest.mark.session_scope_test_warehouse
def test_filter_string_passes(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="variable_name",
        operator=FilterOperators.eq,
        value="foo"
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == "foo"


@pytest.mark.session_scope_test_warehouse
def test_filter_int_to_string_passes(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="variable_name",
        operator=FilterOperators.eq,
        value=10
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == "10"


@pytest.mark.session_scope_test_warehouse
def test_filter_float_passes(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="value",
        operator=FilterOperators.eq,
        value=10.1
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == 10.1


@pytest.mark.session_scope_test_warehouse
def test_filter_int_to_float_passes(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="value",
        operator=FilterOperators.eq,
        value=10
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == 10.0


@pytest.mark.session_scope_test_warehouse
def test_filter_str_to_float_fails(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        ev = session_scope_test_warehouse

        filter = TableFilter(
            column="value",
            operator=FilterOperators.eq,
            value="foo"
        )
        dataframe_schema = ev.primary_timeseries.to_sdf().schema
        filter = validate_filter(filter, dataframe_schema)


@pytest.mark.session_scope_test_warehouse
def test_filter_datetime_passes(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="value_time",
        operator=FilterOperators.eq,
        value=datetime(2021, 1, 1)
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.session_scope_test_warehouse
def test_filter_datetime_passes2(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="value_time",
        operator=FilterOperators.eq,
        value="2021-01-01"
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.session_scope_test_warehouse
def test_filter_datetime_passes3(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="value_time",
        operator=FilterOperators.eq,
        value="2021-01-01T00:00:00"
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.session_scope_test_warehouse
def test_filter_datetime_passes4(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="value_time",
        operator=FilterOperators.eq,
        value=pd.Timestamp("2021-01-01T00:00:00")
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.session_scope_test_warehouse
def test_filter_datetime_passes5(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = session_scope_test_warehouse
    filter = TableFilter(
        column="value_time",
        operator=FilterOperators.eq,
        value="2021-01-01 00:00"
    )
    dataframe_schema = ev.primary_timeseries.to_sdf().schema
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.session_scope_test_warehouse
def test_filter_datetime_fails(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        ev = session_scope_test_warehouse

        filter = TableFilter(
            column="value_time",
            operator=FilterOperators.eq,
            value="10"
        )
        dataframe_schema = ev.primary_timeseries.to_sdf().schema
        filter = validate_filter(filter, dataframe_schema)


@pytest.mark.session_scope_test_warehouse
def test_filter_in_str_fails(session_scope_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        ev = session_scope_test_warehouse

        filter = TableFilter(
            column="configuration",
            operator=FilterOperators.isin,
            value="10"
        )
        dataframe_schema = ev.primary_timeseries.to_sdf().schema
        filter = validate_filter(filter, dataframe_schema)