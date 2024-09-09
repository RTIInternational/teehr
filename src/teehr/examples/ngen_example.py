"""This is an example of how to use TEEHR in NGEN."""
from teehr import Evaluation
from pathlib import Path
import shutil
from teehr import Metrics as metrics


# Set a path to the directory where the evaluation will be created
TEST_STUDY_DIR = Path(Path().home(), "temp", "real_study")
shutil.rmtree(TEST_STUDY_DIR, ignore_errors=True)
TEST_STUDY_DIR.mkdir(parents=True, exist_ok=True)

# Set paths to the locations and crosswalks
# In NGEN this will be provided by NGEN.
TEST_DATA = "/home/matt/repos/teehr/tests/data/two_locations/"
LOCATIONS = Path(TEST_DATA, "two_locations.parquet")
LOCATION_XWALKS = Path(TEST_DATA, "two_crosswalks.parquet")
STARTDATE = "2010-10-01"
ENDDATE = "2020-09-30"

# Create an Evaluation object
eval = Evaluation(dir_path=TEST_STUDY_DIR)

# Enable logging
eval.enable_logging()

# Clone the template
eval.clone_template()

# Load the location data (observations)
# This is a geoparquet file, but any format
# GeoPandas supports is supported.
eval.load.import_locations(in_path=LOCATIONS)

# Load the crosswalk data for USGS to NWM3.0
# Crosswalks are needed for each "simulation source"
# i.e. USGS -> NWM3.0, USGS -> NGEN, etc.
eval.load.import_location_crosswalks(
    in_path=LOCATION_XWALKS
)

# =====>>> This is where USGS -> NGEN crosswalk will be loaded. <<<=====
# eval.load.import_location_crosswalks()

# Load the USGS observations
# Note, dates are hard coded and will be provided by NGEN
eval.fetch.usgs_streamflow(
    start_date=STARTDATE,
    end_date=ENDDATE
)

# Load the NWM retrospective data
eval.fetch.nwm_retrospective_points(
    nwm_version="nwm30",
    variable_name="streamflow",
    start_date=STARTDATE,
    end_date=ENDDATE
)

# =====>>> This is where NGEN simulation output will be loaded. <<<=====
# eval.load.import_secondary_timeseries()

# Create the joined timeseries
eval.create_joined_timeseries(execute_udf=False)

df = eval.metrics.query(
    order_by=["primary_location_id", "configuration_name"],
    group_by=["primary_location_id", "configuration_name"],
    include_metrics=[
        metrics.KlingGuptaEfficiency(),
        metrics.NashSutcliffeEfficiency(),
        metrics.RelativeBias()
    ]
).to_pandas()

df.to_csv(Path(TEST_STUDY_DIR, "metrics.csv"), index=False)
