"""Test the import_location_attributes methods."""
from teehr.loading.location_attributes import (
    convert_single_location_attributes
)
from pathlib import Path
import pytest

from teehr.models.pydantic_table_models import (
    Attribute,
)


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
LOCATION_ATTRIBUTES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "geo", "test_attr_2yr_discharge.csv"
)
GEO_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo")


def test_convert_location_attributes():
    """Test conversion of single location_attributes file."""
    df = convert_single_location_attributes(
        in_filepath=LOCATION_ATTRIBUTES_FILEPATH,
        field_mapping={"attribute_value": "value"}
    )
    assert df.index.size == 3


@pytest.mark.read_write_evaluation_template
def test_validate_and_insert_location_attributes(read_write_evaluation_template):
    """Test the validate location_attributes function."""
    ev = read_write_evaluation_template

    ev.enable_logging()

    ev.locations.load_spatial(
        in_path=GEOJSON_GAGES_FILEPATH,
        location_id_prefix="usgs"
    )
    ev.attributes.add(
        [
            Attribute(
                name="drainage_area",
                type="continuous",
                description="Drainage area in square kilometers"
            ),
            Attribute(
                name="ecoregion",
                type="categorical",
                description="Ecoregion"
            ),
            Attribute(
                name="year_2_discharge",
                type="continuous",
                description="2-yr discharge in cubic meters per second"
            ),
        ]
    )
    ev.location_attributes.load_parquet(
        in_path=GEO_FILEPATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
        location_id_prefix="usgs",
    )