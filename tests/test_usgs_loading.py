"""Test USGS loading."""
from datetime import datetime
from pathlib import Path
import shutil

import pandas as pd

from teehr.loading.usgs.usgs import usgs_to_parquet

TEMP_DIR = Path("tests", "data", "temp", "usgs")


def test_chunkby_location_id(tmp_path):
    """Test chunkby location id."""
    temp_dir = tmp_path
    temp_dir.mkdir(exist_ok=True)

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=temp_dir,
        chunk_by="location_id",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(temp_dir, "02449838.parquet"))
    assert len(df) == 120
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")
    df = pd.read_parquet(Path(temp_dir, "02450825.parquet"))
    assert len(df) == 119
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")


def test_chunkby_day(tmp_path):
    """Test chunkby day."""
    temp_dir = tmp_path
    temp_dir.mkdir(exist_ok=True)

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=temp_dir,
        chunk_by="day",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(temp_dir, "2023-02-20.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(temp_dir, "2023-02-21.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(temp_dir, "2023-02-22.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(temp_dir, "2023-02-23.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(temp_dir, "2023-02-24.parquet"))
    assert len(df) == 47  # missing hour 17
    df = pd.read_parquet(Path(temp_dir, "2023-02-25.parquet"))
    assert len(df) == 48


def test_chunkby_week(tmp_path):
    """Test chunk by week."""
    temp_dir = tmp_path
    temp_dir.mkdir(exist_ok=True)

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 3),
        output_parquet_dir=temp_dir,
        chunk_by="week",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(temp_dir, "2023-02-20_2023-02-26.parquet"))
    assert len(df) == 335
    df = pd.read_parquet(Path(temp_dir, "2023-02-27_2023-03-03.parquet"))
    assert len(df) == 186


def test_chunkby_month(tmp_path):
    """Test chunk by month."""
    temp_dir = tmp_path
    temp_dir.mkdir(exist_ok=True)

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 25),
        output_parquet_dir=temp_dir,
        chunk_by="month",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(temp_dir, "2023-02-20_2023-02-28.parquet"))
    assert len(df) == 431
    df = pd.read_parquet(Path(temp_dir, "2023-03-01_2023-03-25.parquet"))
    assert len(df) == 1111


def test_chunkby_all(tmp_path):
    """Test chunkby all."""
    temp_dir = tmp_path
    temp_dir.mkdir(exist_ok=True)

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=temp_dir,
        overwrite_output=True
    )
    df = pd.read_parquet(Path(temp_dir, "2023-02-20_2023-02-25.parquet"))
    assert len(df) == 241
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-25 00:00:00")


if __name__ == "__main__":
    test_chunkby_location_id(TEMP_DIR)
    test_chunkby_day(TEMP_DIR)
    test_chunkby_week(TEMP_DIR)
    test_chunkby_month(TEMP_DIR)
    test_chunkby_all(TEMP_DIR)
    shutil.rmtree(TEMP_DIR)
