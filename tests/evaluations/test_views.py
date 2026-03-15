"""Tests for the TEEHR View classes."""
import pytest


@pytest.mark.module_scope_test_warehouse
def test_location_attributes_view(module_scope_test_warehouse):
    """Test the LocationAttributesView (pivoted attributes)."""
    ev = module_scope_test_warehouse

    # Test basic pivoted attributes view
    pdf = ev.location_attributes_view().to_pandas()

    assert "location_id" in pdf.columns
    # The test warehouse should have year_2_discharge attribute
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_location_attributes_view_with_filter(module_scope_test_warehouse):
    """Test LocationAttributesView with specific attributes."""
    ev = module_scope_test_warehouse

    # Get only specific attributes
    pdf = ev.location_attributes_view(
        attr_list=["year_2_discharge"]
    ).to_pandas()

    assert "location_id" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    # Should only have location_id and the one attribute
    assert len(pdf.columns) == 2


@pytest.mark.module_scope_test_warehouse
def test_primary_timeseries_view_basic(module_scope_test_warehouse):
    """Test basic PrimaryTimeseriesView."""
    ev = module_scope_test_warehouse

    # Basic view without attributes
    pdf = ev.primary_timeseries_view().to_pandas()

    assert "location_id" in pdf.columns
    assert "value_time" in pdf.columns
    assert "value" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_primary_timeseries_view_with_filter(module_scope_test_warehouse):
    """Test PrimaryTimeseriesView with chained filter."""
    ev = module_scope_test_warehouse

    # Get primary timeseries for one location using chained filter
    pdf = ev.primary_timeseries_view().filter(
        "location_id = 'gage-A'"
    ).to_pandas()

    assert all(pdf['location_id'] == 'gage-A')
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_primary_timeseries_view_with_attrs(module_scope_test_warehouse):
    """Test PrimaryTimeseriesView with location attributes."""
    ev = module_scope_test_warehouse

    # Get primary timeseries with attributes
    pdf = ev.primary_timeseries_view(
        add_attrs=True,
        attr_list=["year_2_discharge"]
    ).to_pandas()

    assert "location_id" in pdf.columns
    assert "value" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_secondary_timeseries_view_basic(module_scope_test_warehouse):
    """Test basic SecondaryTimeseriesView adds primary_location_id."""
    ev = module_scope_test_warehouse

    # Basic view - should add primary_location_id via crosswalk
    pdf = ev.secondary_timeseries_view().to_pandas()

    assert "location_id" in pdf.columns
    assert "primary_location_id" in pdf.columns
    assert "value_time" in pdf.columns
    assert "value" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_secondary_timeseries_view_with_filter(module_scope_test_warehouse):
    """Test SecondaryTimeseriesView with chained filter."""
    ev = module_scope_test_warehouse

    # Get secondary timeseries for one configuration using chained filter
    pdf = ev.secondary_timeseries_view().filter(
        "configuration_name = 'nwm30_retrospective'"
    ).to_pandas()

    assert "primary_location_id" in pdf.columns
    assert all(pdf['configuration_name'] == 'nwm30_retrospective')
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_secondary_timeseries_view_with_attrs(module_scope_test_warehouse):
    """Test SecondaryTimeseriesView with location attributes."""
    ev = module_scope_test_warehouse

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


@pytest.mark.module_scope_test_warehouse
def test_views_chain_operations(module_scope_test_warehouse):
    """Test that views can be chained with filter, order_by, etc."""
    ev = module_scope_test_warehouse

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


@pytest.mark.module_scope_test_warehouse
def test_add_attributes_primary_timeseries(module_scope_test_warehouse):
    """Test add_attributes on primary timeseries (joins on location_id)."""
    ev = module_scope_test_warehouse

    pdf = (
        ev.primary_timeseries
        .add_attributes(attr_list=["year_2_discharge"])
        .to_pandas()
    )

    assert "location_id" in pdf.columns
    assert "value" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_add_attributes_all(module_scope_test_warehouse):
    """Test add_attributes with no attr_list adds all attributes."""
    ev = module_scope_test_warehouse

    pdf = (
        ev.primary_timeseries
        .add_attributes()
        .to_pandas()
    )

    assert "location_id" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_add_attributes_with_explicit_location_id_col(module_scope_test_warehouse):
    """Test add_attributes with explicit location_id_col on secondary view."""
    ev = module_scope_test_warehouse

    # Secondary timeseries view has primary_location_id - join attrs on it
    pdf = (
        ev.secondary_timeseries_view()
        .add_attributes(
            attr_list=["year_2_discharge"],
            location_id_col="primary_location_id"
        )
        .to_pandas()
    )

    assert "location_id" in pdf.columns
    assert "primary_location_id" in pdf.columns
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0


@pytest.mark.module_scope_test_warehouse
def test_add_attributes_chained(module_scope_test_warehouse):
    """Test add_attributes chained with filter."""
    ev = module_scope_test_warehouse

    pdf = (
        ev.primary_timeseries
        .add_attributes(attr_list=["year_2_discharge"])
        .filter("location_id = 'gage-A'")
        .to_pandas()
    )

    assert all(pdf['location_id'] == 'gage-A')
    assert "year_2_discharge" in pdf.columns
    assert len(pdf) > 0

