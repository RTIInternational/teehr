"""Tests for Iceberg."""
from pathlib import Path
import shutil
import tempfile

import teehr
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_rewrite_table_paths(tmpdir):
    """Test registering a local warehouse."""
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
    ev.rewrite_table_paths()

    assert ev.units.to_pandas().shape == (4, 2)



if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tmpdir:
        test_rewrite_table_paths(
            tempfile.mkdtemp(
                prefix="0-",
                dir=tmpdir
            )
        )