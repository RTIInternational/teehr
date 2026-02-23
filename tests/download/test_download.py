"""Test downloading from the S3 warehouse via the TEEHR API."""
from pathlib import Path
import pytest
from teehr.evaluation.evaluation import Evaluation



@pytest.mark.function_scope_evaluation_template
def test_downloading(function_scope_evaluation_template):
    """Test downloading from the S3 warehouse via the TEEHR API."""
    ev = function_scope_evaluation_template

    # This is a bit of a hack to test the download function without
    # actually running the full evaluation. We just want to make sure
    # the download function runs and creates the expected files.
    gdf = ev.download.get_locations(
        location_id_prefix="usgs",
        include_attributes=True,
        limit=10
    )

    pass