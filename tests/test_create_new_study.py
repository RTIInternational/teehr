"""Tests for the TEEHR study creation."""
from pathlib import Path
# import shutil
import tempfile


def test_clone_template(tmpdir):
    """Test creating a new study."""
    from teehr import Evaluation

    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    # Not a complete test, but at least we know the function runs.
    assert Path(tmpdir, "dataset").is_dir()
    assert Path(tmpdir, "cache").is_dir()
    assert Path(tmpdir, "scripts").is_dir()
    assert Path(tmpdir, ".gitignore").is_file()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_clone_template(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
