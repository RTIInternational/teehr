"""Test the import_location_attributes methods."""
from teehr.loading.location_attributes import convert_location_attributes
from pathlib import Path
from teehr import Evaluation
import tempfile
from teehr.models.loading.domain_tables import (
    Attribute,
)


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
LOCATION_ATTRIBUTES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "geo", "test_attr_2yr_discharge.csv"
)
GEO_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo")


def test_convert_location_attributes(tmpdir):
    """Test conversion of single location_attributes file."""
    output_filepath = Path(tmpdir, "test_attr_2yr_discharge.parquet")

    convert_location_attributes(
        in_path=LOCATION_ATTRIBUTES_FILEPATH,
        out_dirpath=tmpdir,
        field_mapping={"attribute_value": "value"}
    )
    assert output_filepath.is_file()


def test_validate_and_insert_location_attributes(tmpdir):
    """Test the validate location_attributes function."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    eval.enable_logging()

    eval.load.import_locations(
        in_path=GEOJSON_GAGES_FILEPATH
    )
    eval.load.add_attribute(
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
    eval.load.import_location_attributes(
        in_path=GEO_FILEPATH,
        field_mapping={"attribute_value": "value"},
        pattern="test_attr_*.parquet",
    )

    assert True


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_convert_location_attributes(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_validate_and_insert_location_attributes(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
