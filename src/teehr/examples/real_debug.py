from teehr import Evaluation
from pathlib import Path

# Set a path to the directory where the evaluation will be created
TEST_STUDY_DIR = Path(Path().home(), "temp", "real_study")
TEST_STUDY_DIR.mkdir(parents=True, exist_ok=True)

TEST_DATA = "/home/matt/repos/teehr/tests/data/two_locations/"
LOCATIONS = Path(TEST_DATA, "two_locations.parquet")
XWALKS = Path(TEST_DATA, "two_crosswalks.parquet")

# Create an Evaluation object
eval = Evaluation(dir_path=TEST_STUDY_DIR)

# Enable logging
eval.enable_logging()

# Modify
eval.create_joined_timeseries(execute_udf=True)