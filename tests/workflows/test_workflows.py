"""Tests for Evaluation workflows."""
import tempfile

import teehr


def test_ngiab_teehr_mechanics(tmpdir):
    """Test running a workflow class."""
    ev = teehr.Evaluation(local_warehouse_dir=tmpdir, create_local_dir=True)
    ev.clone_template()

    ev.workflows.ngiab_teehr()

    pass


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_ngiab_teehr_mechanics(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
