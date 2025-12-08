import teehr
from pathlib import Path
import pandas as pd
from shapely.geometry import Point
import geopandas as gpd

TEST_STUDY_DATA_DIR_V0_5 = Path("tests", "data", "v0_5_ensemble_study")


def setup_v0_5_ensemble_study(tmpdir):
    """Create a test evaluation with ensemble forecasts using teehr."""
    # define pathing
    location_xwalk_path = TEST_STUDY_DATA_DIR_V0_5 / "location_crosswalks.parquet"
    primary_ts_path = TEST_STUDY_DATA_DIR_V0_5 / "primary_timeseries.parquet"
    secondary_ts_path = TEST_STUDY_DATA_DIR_V0_5 / "secondary_timeseries.parquet"
    configurations_path = TEST_STUDY_DATA_DIR_V0_5 / "configurations.parquet"
    variables_path = TEST_STUDY_DATA_DIR_V0_5 / "variables.parquet"

    # initialize evaluation
    ev = teehr.Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    # create locations
    location_dict = {
        'id': 'obs-GILN6',
        'name': 'Schoharie Creek at Schoharie',
        'geometry': Point(-74.45043182373047, 42.397300720214844),
    }
    gdf = gpd.GeoDataFrame(
        [location_dict],
        geometry='geometry',
        crs="EPSG:4269"
        )
    ev.locations.load_dataframe(df=gdf, write_mode="overwrite")

    # load crosswalk
    ev.location_crosswalks.load_parquet(
        in_path=location_xwalk_path
    )

    # add configurations
    df = pd.read_parquet(configurations_path)
    for _, row in df.iterrows():
        ev.configurations.add(
            teehr.Configuration(
                name=row["name"],
                type=row["type"],
                description=row["description"]
            )
        )

    # add variables table
    df = pd.read_parquet(variables_path)
    for _, row in df.iterrows():
        ev.variables.add(
            teehr.Variable(
                name=row["name"],
                long_name=row["long_name"],
            )
        )

    # load primary timeseries
    ev.primary_timeseries.load_parquet(
        in_path=primary_ts_path
    )

    # load secondary timeseries
    ev.secondary_timeseries.load_parquet(
        in_path=secondary_ts_path
    )

    # create JTS
    ev.joined_timeseries.create(add_attrs=False, execute_scripts=True)

    return ev
