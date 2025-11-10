"""Fixtures for v0.3 study tests."""
from pathlib import Path
from teehr import Evaluation

import tarfile
import os
import shutil
import logging

logger = logging.getLogger(__name__)

TEST_DATA_FILE = Path("tests", "data", "v0_3_study_test.tar.gz")


def setup_v0_3_study(tmpdir):
    """Set up a v0.3 study."""
    shutil.copyfile(TEST_DATA_FILE, Path(tmpdir, "v0_3_study_test.tar.gz"))

    logger.info("Extracting archive...")
    with tarfile.open(Path(tmpdir, "v0_3_study_test.tar.gz"), 'r:gz') as tar:
        tar.extractall(path=tmpdir)
    logger.info("✅ Extraction complete")

    os.remove(Path(tmpdir, "v0_3_study_test.tar.gz"))
    logger.info(f"✅ Removed archive {tmpdir}")

    ev = Evaluation(
        dir_path=Path(tmpdir, "v0_3_study_test"),
        create_dir=False
    )

    return ev


if __name__ == "__main__":
    setup_v0_3_study("/home/slamont/temp/v0_3_study_test")