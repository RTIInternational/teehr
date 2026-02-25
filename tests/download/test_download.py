"""Test downloading from the S3 warehouse via the TEEHR API."""
from pathlib import Path
import pytest
from teehr.evaluation.evaluation import Evaluation



@pytest.mark.module_scope_evaluation_template
def test_download_locations(module_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = module_scope_evaluation_template
    gdf = ev.download.locations(
        prefix="usgs",
        include_attributes=False,
        limit=1
    )
    assert len(gdf) == 1
    assert "id" in gdf.columns
    assert "geometry" in gdf.columns
    assert "name" in gdf.columns


@pytest.mark.module_scope_evaluation_template
def test_download_and_load_locations(module_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = module_scope_evaluation_template
    ev.download.locations(
        prefix="usgs",
        include_attributes=True,
        limit=1,
        load=True
    )
    sdf = ev.locations.to_sdf()
    assert sdf.count() == 1
    assert "id" in sdf.columns
    assert "geometry" in sdf.columns
    assert "name" in sdf.columns


@pytest.mark.module_scope_evaluation_template
def test_download_evaluation_subset(module_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = module_scope_evaluation_template
    ev.download.evaluation_subset(
        location_ids=["usgs-06892350"],
        start_date="2020-01-01",
        end_date="2020-01-02",
        primary_configuration_name="usgs_observations",
        secondary_configuration_name="nwm30_retrospective"
    )
    assert ev.locations.to_sdf().count() == 1
    assert ev.location_attributes.to_sdf().count() == 22
    assert ev.units.to_sdf().count() == 4
    assert ev.variables.to_sdf().count() == 5
    assert ev.attributes.to_sdf().count() == 50
    assert ev.configurations.to_sdf().count() == 2
    assert ev.primary_timeseries.to_sdf().count() == 25
    assert ev.secondary_timeseries.to_sdf().count() == 25