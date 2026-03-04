"""Tests for the TEEHR View classes."""
import pytest


@pytest.mark.function_scope_test_warehouse
def test_location_attributes_view(function_scope_test_warehouse):
    """Test the LocationAttributesView (pivoted attributes)."""
    ev = function_scope_test_warehouse

    # Test basic pivoted attributes view
    pdf = ev.location_attributes_view().to_pandas()

    assert "location_id" in pdf.columns
    # The test warehouse should have year_2_discharge attribute
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.function_scope_test_warehouse
def test_location_attributes_view_with_filter(function_scope_test_warehouse):
    """Test LocationAttributesView with specific attributes."""
    ev = function_scope_test_warehouse

    # Get only specific attributes
    pdf = ev.location_attributes_view(
        attr_list=["year_2_discharge"]
    ).to_pandas()

    assert "location_id" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    # Should only have location_id and the one attribute
    assert len(pdf.columns) == 2


@pytest.mark.function_scope_test_warehouse
def test_primary_timeseries_view_basic(function_scope_test_warehouse):
    """Test basic PrimaryTimeseriesView."""
    ev = function_scope_test_warehouse

    # Basic view without attributes
    pdf = ev.primary_timeseries_view().to_pandas()

    assert "location_id" in pdf.columns
    assert "value_time" in pdf.columns
    assert "value" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.function_scope_test_warehouse
def test_primary_timeseries_view_with_filter(function_scope_test_warehouse):
    """Test PrimaryTimeseriesView with chained filter."""
    ev = function_scope_test_warehouse

    # Get primary timeseries for one location using chained filter
    pdf = ev.primary_timeseries_view().filter(
        "location_id = 'gage-A'"
    ).to_pandas()

    assert all(pdf['location_id'] == 'gage-A')
    assert len(pdf) > 0


@pytest.mark.function_scope_test_warehouse
def test_primary_timeseries_view_with_attrs(function_scope_test_warehouse):
    """Test PrimaryTimeseriesView with location attributes."""
    ev = function_scope_test_warehouse

    # Get primary timeseries with attributes
    pdf = ev.primary_timeseries_view(
        add_attrs=True,
        attr_list=["year_2_discharge"]
    ).to_pandas()

    assert "location_id" in pdf.columns
    assert "value" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.function_scope_test_warehouse
def test_secondary_timeseries_view_basic(function_scope_test_warehouse):
    """Test basic SecondaryTimeseriesView adds primary_location_id."""
    ev = function_scope_test_warehouse

    # Basic view - should add primary_location_id via crosswalk
    pdf = ev.secondary_timeseries_view().to_pandas()

    assert "location_id" in pdf.columns
    assert "primary_location_id" in pdf.columns
    assert "value_time" in pdf.columns
    assert "value" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.function_scope_test_warehouse
def test_secondary_timeseries_view_with_filter(function_scope_test_warehouse):
    """Test SecondaryTimeseriesView with chained filter."""
    ev = function_scope_test_warehouse

    # Get secondary timeseries for one configuration using chained filter
    pdf = ev.secondary_timeseries_view().filter(
        "configuration_name = 'nwm30_retrospective'"
    ).to_pandas()

    assert "primary_location_id" in pdf.columns
    assert all(pdf['configuration_name'] == 'nwm30_retrospective')
    assert len(pdf) > 0


@pytest.mark.function_scope_test_warehouse
def test_secondary_timeseries_view_with_attrs(function_scope_test_warehouse):
    """Test SecondaryTimeseriesView with location attributes."""
    ev = function_scope_test_warehouse

    # Get secondary timeseries with attributes
    pdf = ev.secondary_timeseries_view(
        add_attrs=True,
        attr_list=["year_2_discharge"]
    ).to_pandas()

    assert "location_id" in pdf.columns
    assert "primary_location_id" in pdf.columns
    assert "value" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.function_scope_test_warehouse
def test_views_chain_operations(function_scope_test_warehouse):
    """Test that views can be chained with filter, order_by, etc."""
    ev = function_scope_test_warehouse

    # Chain operations on primary timeseries view
    pdf = (
        ev.primary_timeseries_view()
        .filter("location_id = 'gage-A'")
        .order_by(["value_time"])
        .to_pandas()
    )

    assert all(pdf['location_id'] == 'gage-A')
    # Check ordering
    assert pdf['value_time'].is_monotonic_increasing
