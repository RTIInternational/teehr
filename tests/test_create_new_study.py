"""Tests for the TEEHR study creation."""
from pathlib import Path
import shutil


def test_clone_template():
    """Test creating a new study."""
    from teehr import Evaluation

    TEST_STUDY_DIR = Path("tests", "data", "temp", "test_create_study")
    shutil.rmtree(TEST_STUDY_DIR)
    TEST_STUDY_DIR.mkdir()

    eval = Evaluation(dir_path=TEST_STUDY_DIR)
    eval.clone_template()

    # Not a complete test, but at least we know the function runs.
    assert Path(TEST_STUDY_DIR, "database").is_dir()
    assert Path(TEST_STUDY_DIR, "temp").is_dir()
    assert Path(TEST_STUDY_DIR, "scripts").is_dir()
    assert Path(TEST_STUDY_DIR, ".gitignore").is_file()


if __name__ == "__main__":
    test_clone_template()
