"""This module tests the filter validation function."""
from teehr import Evaluation
import tempfile


def test_chain_filter_single(tmpdir):
    """Test filter string."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()
    units_df = eval.units.filter("name = 'm^3/s'").to_df()
    assert len(units_df) == 1


def test_dict_parse(tmpdir):
    """Test dict parse to model."""
    eval = Evaluation(dir_path=tmpdir)
    eval.clone_template()

    units_df = eval.units.query(
        filters=[{
            "column": "name",
            "operator": "=",
            "value": "m^3/s"
        }]
    ).to_df()
    assert len(units_df) == 1


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_chain_filter_single(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_dict_parse(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )