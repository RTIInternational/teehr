"""This module tests the filter validation function."""
from teehr import Evaluation
import tempfile


def test_filter_string(tmpdir):
    """Test xxx."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    units_df = eval.units.filter("name = 'm^3/s'").to_df()
    assert len(units_df) == 1


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_filter_string(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )