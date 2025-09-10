"""Test the import locations functionality."""
from teehr.loading.locations import convert_locations
from pathlib import Path
from teehr import Evaluation
import tempfile


TEST_STUDY_DATA_DIR = Path("tests", "data", "v0_3_test_study")
GEOJSON_GAGES_FILEPATH = Path(TEST_STUDY_DATA_DIR, "geo", "gages.geojson")
GEOJSON_NP_GAGES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR, "geo", "gages_no_prefix.geojson"
)


def test_convert_locations_geojson(tmpdir):
    """Test the convert_locations function on geojson."""
    output_filepath = Path(tmpdir, "gages.parquet")

    convert_locations(
        in_path=GEOJSON_GAGES_FILEPATH,
        out_dirpath=tmpdir,
    )
    assert output_filepath.is_file()


def test_validate_and_insert_locations(tmpdir):
    """Test the validate_locations function."""
    ev = Evaluation(dir_path=tmpdir, create_dir=True)
    ev.clone_template()
    # Load and replace the location ID prefix
    ev.locations.load_spatial(
        in_path=GEOJSON_GAGES_FILEPATH,
        location_id_prefix="test"
    )
    # Append additional location
    ev.locations.load_spatial(
        in_path="tests/data/two_locations/two_locations.parquet",
    )
    # Now update existing 'test' locations with new names
    # and add a few more (upsert).
    ev.locations.load_spatial(
        in_path=Path(TEST_STUDY_DATA_DIR, "geo", "extended_v03_gages.geojson"),
        location_id_prefix="test",
        write_mode="upsert",
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
    ev.spark.stop()


def test_validate_and_insert_locations_adding_prefix(tmpdir):
    """Test the validate_locations function."""
    ev = Evaluation(dir_path=tmpdir, create_dir=True)
    ev.clone_template()

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
    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_convert_locations_geojson(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_validate_and_insert_locations(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_validate_and_insert_locations_adding_prefix(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
