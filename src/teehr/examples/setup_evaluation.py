"""Set up and clone a new evaluation study."""
from teehr import Evaluation
from pathlib import Path
import logging

TEST_STUDY_DIR = Path(Path().home(), "temp", "test_create_study")
TEST_STUDY_DIR.mkdir()

logger = logging.getLogger("teehr")
logger.addHandler(logging.StreamHandler())
logger.addHandler(logging.FileHandler(Path(TEST_STUDY_DIR, 'teehr.log')))
logger.setLevel(logging.DEBUG)

eval = Evaluation(dir_path=TEST_STUDY_DIR)
eval.clone_template()