import pandas as pd
from pathlib import Path
from teehr.loading.generate_weights import generate_weights_file

TEST_DIR = Path("tests", "data")
TEMPLATE_FILEPATH = Path(TEST_DIR, "test_template_grid.nc")
ZONES_FILEPATH = Path(TEST_DIR, "test_ngen_divides.parquet")
WEIGHTS_FILEPATH = Path(TEST_DIR, "test_weights_results.parquet")


def test_weights():
    df = generate_weights_file(
        zone_polygon_filepath=ZONES_FILEPATH,
        template_dataset=TEMPLATE_FILEPATH,
        variable_name="RAINRATE",
        output_weights_filepath=None,
        unique_zone_id="id",
    )

    df_test = pd.read_parquet(WEIGHTS_FILEPATH)

    assert df.equals(df_test)


if __name__ == "__main__":
    test_weights()
    pass
