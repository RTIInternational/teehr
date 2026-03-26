"""Test downloading from the S3 warehouse via the TEEHR API."""
import pytest


@pytest.mark.function_scope_evaluation_template
def test_download_locations_by_ids(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template
    gdf = ev.download.locations(
        ids=["usgs-02424000", "usgs-03068800"],
        include_attributes=False,
    )
    assert len(gdf) == 2
    assert "id" in gdf.columns
    assert "geometry" in gdf.columns
    assert "name" in gdf.columns


@pytest.mark.function_scope_evaluation_template
def test_download_evaluation_subset(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template
    ev.download.evaluation_subset(
        location_ids="usgs-03068800",
        start_date="2020-01-01",
        end_date="2020-01-02",
        primary_configuration_name="usgs_observations",
        secondary_configuration_name="nwm30_retrospective"
    )
    assert ev.locations.to_sdf().count() == 1
    assert ev.location_attributes.to_sdf().count() >= 48
    assert ev.units.to_sdf().count() >= 5
    assert ev.variables.to_sdf().count() >= 5
    assert ev.attributes.to_sdf().count() >= 50
    assert ev.configurations.to_sdf().count() == 2
    assert ev.primary_timeseries.to_sdf().count() == 25
    assert ev.secondary_timeseries.to_sdf().count() == 25


@pytest.mark.function_scope_evaluation_template
@pytest.mark.skip(reason="This test is meant to be run manually against a local TEEHR API.")
def test_download_from_local_primary_timeseries(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template

    ev.download.configure(
        api_base_url="https://api.teehr.local.app.garden",
        verify_ssl=False,
    )
    df = ev.download.primary_timeseries(
        primary_location_id="usgs-03068800",
        start_date="2020-01-01",
        end_date="2020-02-01",
        configuration_name="usgs_observations",
        variable_name="streamflow_hourly_inst",
    )

    assert len(df) == 697


@pytest.mark.function_scope_evaluation_template
@pytest.mark.skip(reason="This test is meant to be run manually against a local TEEHR API.")
def test_download_from_local_primary_timeseries_pagination(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template

    ev.download.configure(
        api_base_url="https://api.teehr.local.app.garden",
        verify_ssl=False,
    )
    df = ev.download.primary_timeseries(
        primary_location_id="usgs-03068800",
        start_date="2020-01-01",
        end_date="2020-02-01",
        configuration_name="usgs_observations",
        variable_name="streamflow_hourly_inst",
        page_size=100,
    )

    assert len(df) == 697
