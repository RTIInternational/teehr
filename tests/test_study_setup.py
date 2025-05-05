"""Tests for basic setup."""
import tempfile

from setup_v0_3_study import setup_v0_3_study


def test_setup(tmpdir):
    """Test the format_filter_to_str function with eq_str."""
    setup_v0_3_study(tmpdir)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_setup(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )