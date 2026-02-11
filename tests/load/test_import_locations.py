"""Test the import locations functionality."""
from teehr.loading.locations import convert_single_locations
from pathlib import Path
import pytest


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_warehouse_data")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
GEOJSON_NP_GAGES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "geo", "gages_no_prefix.geojson"
)


def test_convert_locations_geojson():
    """Test the convert_locations function on geojson."""
    df = convert_single_locations(
        in_filepath=GEOJSON_GAGES_FILEPATH,
        field_mapping={"id": "id"}
    )
    assert df.index.size == 3


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_locations(function_scope_evaluation_template):
    """Test the validate_locations function."""
    ev = function_scope_evaluation_template

    # test constant field values
    constant_field_values = {
        "name": "test",
    }

    # Load and replace the location ID prefix
    ev.locations.load_spatial(
        in_path=GEOJSON_GAGES_FILEPATH,
        location_id_prefix="test",
        constant_field_values=constant_field_values,
    )
    # Append additional location
    ev.locations.load_spatial(
        in_path=Path(TEST_STUDY_DATA_DIR, "geo", "two_locations.parquet"),
        constant_field_values=constant_field_values,
    )
    # Now update existing 'test' locations with new names
    # and add a few more (upsert).
    ev.locations.load_spatial(
        in_path=Path(TEST_STUDY_DATA_DIR, "geo", "extended_v03_gages.geojson"),
        location_id_prefix="test",
        write_mode="upsert",
        constant_field_values=constant_field_values,
    )
    assert sorted(ev.locations.to_pandas()["id"].tolist()) == [
        "test-A",
        "test-B",
        "test-C",
        "test-D",
        "test-E",
        "test-F",
        "usgs-14138800",
        "usgs-14316700"
    ]
    assert ev.locations.to_sdf().count() == 8
    # check that all names are "test"
    assert set(ev.locations.to_pandas()["name"].tolist()) == {"test"}


@pytest.mark.function_scope_evaluation_template
def test_validate_and_insert_locations_adding_prefix(function_scope_evaluation_template):
    """Test the validate_locations function."""
    ev = function_scope_evaluation_template

    # Add a new location ID prefix
    ev.locations.load_spatial(
        in_path=GEOJSON_NP_GAGES_FILEPATH,
        location_id_prefix="test"
    )

    id_val = (
                ev.
                locations.
                to_sdf().
                select("id").
                first().
                asDict()["id"].
                split("-")
            )

    assert ev.locations.to_sdf().count() == 3
    assert id_val[0] == "test"
    assert id_val[1] == "A"