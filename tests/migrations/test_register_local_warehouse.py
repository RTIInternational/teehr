"""Tests for Iceberg."""
from pathlib import Path
import shutil
import tempfile

import teehr
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study_from_scratch  # noqa


def test_register_local_warehouse(tmpdir):
    """Test registering a local warehouse."""
    # setup_v0_3_study_from_scratch(tmpdir)


    # Extract pre-created warehouse and recreate Iceberg tables from data files
    test_data_dir = Path.cwd() / "tests" / "data" / "v0_3_test_study"
    tar_file = test_data_dir / "local_warehouse_jdbc.tar.gz"
    # Unpack to a temporary location to access data files
    temp_extract_dir = Path(tmpdir) / "temp_warehouse"
    shutil.unpack_archive(tar_file, temp_extract_dir)

    ev = teehr.Evaluation(
        dir_path=temp_extract_dir,
    )

    # Register the local warehouse.
    ev.register_warehouse()

    pass



if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tmpdir:
        test_register_local_warehouse(
            tempfile.mkdtemp(
                prefix="0-",
                dir=tmpdir
            )
        )