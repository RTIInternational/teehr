"""This module tests the filter validation function."""
from datetime import datetime
from teehr.models.filters import (
    TimeseriesFilter,
    FilterOperators
)
import pandas as pd
from teehr.querying.filter_format import validate_filter
import pytest


@pytest.mark.read_only_test_warehouse
def test_filter_string_passes(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.eq,
        value="foo"
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == "foo"


@pytest.mark.read_only_test_warehouse
def test_filter_int_to_string_passes(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.variable_name,
        operator=FilterOperators.eq,
        value=10
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == "10"


@pytest.mark.read_only_test_warehouse
def test_filter_float_passes(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperators.eq,
        value=10.1
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == 10.1


@pytest.mark.read_only_test_warehouse
def test_filter_int_to_float_passes(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value,
        operator=FilterOperators.eq,
        value=10
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == 10.0


@pytest.mark.read_only_test_warehouse
def test_filter_str_to_float_fails(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        ev = read_only_test_warehouse
        fields = ev.primary_timeseries.field_enum()
        filter = TimeseriesFilter(
            column=fields.value,
            operator=FilterOperators.eq,
            value="foo"
        )
        dataframe_schema = ev.primary_timeseries._get_schema("pandas")
        filter = validate_filter(filter, dataframe_schema)


@pytest.mark.read_only_test_warehouse
def test_filter_datetime_passes(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value=datetime(2021, 1, 1)
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.read_only_test_warehouse
def test_filter_datetime_passes2(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value="2021-01-01"
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.read_only_test_warehouse
def test_filter_datetime_passes3(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value="2021-01-01T00:00:00"
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.read_only_test_warehouse
def test_filter_datetime_passes4(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value=pd.Timestamp("2021-01-01T00:00:00")
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.read_only_test_warehouse
def test_filter_datetime_passes5(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    ev = read_only_test_warehouse
    fields = ev.primary_timeseries.field_enum()
    filter = TimeseriesFilter(
        column=fields.value_time,
        operator=FilterOperators.eq,
        value="2021-01-01 00:00"
    )
    dataframe_schema = ev.primary_timeseries._get_schema("pandas")
    filter = validate_filter(filter, dataframe_schema)
    assert filter.value == datetime(2021, 1, 1)


@pytest.mark.read_only_test_warehouse
def test_filter_datetime_fails(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        ev = read_only_test_warehouse
        fields = ev.primary_timeseries.field_enum()
        filter = TimeseriesFilter(
            column=fields.value_time,
            operator=FilterOperators.eq,
            value="10"
        )
        dataframe_schema = ev.primary_timeseries._get_schema("pandas")
        filter = validate_filter(filter, dataframe_schema)


@pytest.mark.read_only_test_warehouse
def test_filter_in_str_fails(read_only_test_warehouse):
    """Test the format_filter_to_str function with eq_str."""
    with pytest.raises(Exception):
        ev = read_only_test_warehouse
        fields = ev.primary_timeseries.field_enum()
        filter = TimeseriesFilter(
            column=fields.configuration,
            operator=FilterOperators.isin,
            value="10"
        )
        dataframe_schema = ev.primary_timeseries._get_schema("pandas")
        filter = validate_filter(filter, dataframe_schema)