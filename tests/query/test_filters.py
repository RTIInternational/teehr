"""This module tests the filter functions on primary_timeseries.

This module tests the filter functions on primary_timeseries. It
should apply to all tables.
"""
from datetime import timedelta
import pytest
from teehr import RowLevelCalculatedFields as rcf
from teehr.models.filters import (
    FilterOperators,
)
from teehr.models.filters import TableFilter

@pytest.mark.function_scope_two_location_warehouse
def test_filtering_a_new_table(function_scope_two_location_warehouse):
    """Test filtering a new table with TableFilter."""
    ev = function_scope_two_location_warehouse
    ev.write.to_warehouse(
        table_name="new_attributes",
        source_data=ev.attributes.to_sdf(),
        write_mode="create_or_replace"
    )
    attr_filter = TableFilter(
        column="name",
        operator=FilterOperators.eq,
        value="frac_urban"
    )
    df1 = ev.table(table_name="new_attributes").filter("name = 'frac_urban'").to_pandas()
    df2 = ev.table(table_name="new_attributes").filter(attr_filter).to_pandas()
    assert len(df1) == 1
    assert len(df2) == 1


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_single_str(module_scope_test_warehouse):
    """Test filter string."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.filter("location_id = 'gage-A'").to_pandas()
    assert len(df) == 26


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_single_str2(module_scope_test_warehouse):
    """Test filter string with invalid id."""
    ev = module_scope_test_warehouse
    with pytest.raises(Exception):
        ev.primary_timeseries.filter("id = 'gage-A'").to_pandas()


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_single_dict(module_scope_test_warehouse):
    """Test filter dict."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.filter({
        "column": "location_id",
        "operator": "=",
        "value": "gage-A"
    }).to_pandas()
    assert len(df) == 26


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_single_dict2(module_scope_test_warehouse):
    """Test filter dict with invalid id."""
    ev = module_scope_test_warehouse
    with pytest.raises(Exception):
        ev.primary_timeseries.filter({
            "column": "id",
            "operator": "=",
            "value": "gage-A"
        }).to_pandas()


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_single_model(module_scope_test_warehouse):
    """Test filter model."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.filter(
        TableFilter(
            column="location_id",
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_single_model2(module_scope_test_warehouse):
    """Test filter model."""
    ev = module_scope_test_warehouse
    with pytest.raises(Exception):
        ev.primary_timeseries.filter(
            TableFilter(
                column="id",
                operator=FilterOperators.eq,
                value="gage-A"
            )
        ).to_pandas()


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_list_str(module_scope_test_warehouse):
    """Test filter list of strings."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.filter([
        "location_id = 'gage-A'",
        "value_time > '2022-01-01T12:00:00'"
    ]).to_pandas()
    assert len(df) == 13


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_list_dict(module_scope_test_warehouse):
    """Test filter list of dicts."""
    ev = module_scope_test_warehouse
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


@pytest.mark.module_scope_test_warehouse
def test_chain_filter_list_model(module_scope_test_warehouse):
    """Test filter list of models."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.filter([
        TableFilter(
            column="location_id",
            operator=FilterOperators.eq,
            value="gage-A"
        ),
        TableFilter(
            column="value_time",
            operator=FilterOperators.gt,
            value="2022-01-01T12:00:00Z"
        )
    ]).to_pandas()
    assert len(df) == 13


@pytest.mark.module_scope_test_warehouse
def test_query_single_str(module_scope_test_warehouse):
    """Test query string."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters="location_id = 'gage-A'"
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.module_scope_test_warehouse
def test_query_single_dict(module_scope_test_warehouse):
    """Test query dict."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters={
            "column": "location_id",
            "operator": "=",
            "value": "gage-A"
        }
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.module_scope_test_warehouse
def test_query_single_model(module_scope_test_warehouse):
    """Test query model."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters=TableFilter(
            column="location_id",
            operator=FilterOperators.eq,
            value="gage-A"
        )
    ).to_pandas()
    assert len(df) == 26


@pytest.mark.module_scope_test_warehouse
def test_query_list_str(module_scope_test_warehouse):
    """Test query list of strings."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters=[
            "location_id = 'gage-A'",
            "value_time > '2022-01-01T12:00:00'"
        ]
    ).to_pandas()
    assert len(df) == 13


@pytest.mark.module_scope_test_warehouse
def test_query_list_dict(module_scope_test_warehouse):
    """Test query list of dicts."""
    ev = module_scope_test_warehouse
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


@pytest.mark.module_scope_test_warehouse
def test_query_list_model(module_scope_test_warehouse):
    """Test query list of models."""
    ev = module_scope_test_warehouse
    df = ev.primary_timeseries.query(
        filters=[
            TableFilter(
                column="location_id",
                operator=FilterOperators.eq,
                value="gage-A"
            ),
            TableFilter(
                column="value_time",
                operator=FilterOperators.gt,
                value="2022-01-01T12:00:00Z"
            )
        ]
    ).to_pandas()
    assert len(df) == 13


@pytest.mark.function_scope_test_warehouse
def test_filter_by_lead_time(function_scope_test_warehouse):
    """Test filter by lead time."""
    ev = function_scope_test_warehouse
    ev.joined_timeseries_view().add_calculated_fields([
        rcf.ForecastLeadTime(),
    ]).write("joined_timeseries")

    filter_value = timedelta(days=0, hours=18)
    df = ev.table("joined_timeseries").query(
            TableFilter(
                column="forecast_lead_time",
                operator=FilterOperators.gt,
                value=filter_value
            )
    ).to_pandas()
    assert len(df) == 45

    df = ev.table("joined_timeseries").filter(
        filters=[
            {
                "column": "forecast_lead_time",
                "operator": ">",
                "value": filter_value
            }
        ]
    ).to_pandas()
    assert len(df) == 45

    df = ev.table("joined_timeseries").filter(
        "forecast_lead_time > interval 18 hours"
    ).to_pandas()
    assert len(df) == 45

    df = ev.table("joined_timeseries").filter(
        "forecast_lead_time < interval 1 day"
    ).to_pandas()
    assert len(df) == 216

    df = ev.table("joined_timeseries").filter(
        "forecast_lead_time < interval 3600 seconds"
    ).to_pandas()
    assert len(df) == 9
