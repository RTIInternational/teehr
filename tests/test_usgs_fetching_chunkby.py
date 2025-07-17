"""Test USGS fetching."""
from datetime import datetime
from pathlib import Path
import tempfile

import pandas as pd

from teehr.fetching.usgs.usgs import usgs_to_parquet


def test_chunkby_location_id(tmpdir):
    """Test chunkby location id."""
    usgs_to_parquet(
        sites=[
            "08025360",  # Reservoir will be skipped
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=Path(tmpdir),
        chunk_by="location_id",
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(
            tmpdir,
            "02449838.parquet"
        )
    )
    assert len(df) == 120
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")
    df = pd.read_parquet(
        Path(
            tmpdir,
            "02450825.parquet"
        )
    )
    assert len(df) == 119
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")


def test_chunkby_day(tmpdir):
    """Test chunkby day."""
    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=Path(tmpdir),
        chunk_by="day",
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-20.parquet"
        )
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-21.parquet"
        )
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-22.parquet"
        )
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-23.parquet"
        )
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-24.parquet"
        )
    )
    assert len(df) == 47  # missing hour 17
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-25.parquet"
        )
    )
    assert len(df) == 48


def test_chunkby_week(tmpdir):
    """Test chunk by week."""
    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 3),
        output_parquet_dir=Path(tmpdir),
        chunk_by="week",
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-20_2023-02-26.parquet"
        )
    )
    assert len(df) == 335
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-27_2023-03-03.parquet"
        )
    )
    assert len(df) == 186


def test_chunkby_month(tmpdir):
    """Test chunk by month."""
    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 25),
        output_parquet_dir=Path(tmpdir),
        chunk_by="month",
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-20_2023-02-28.parquet"
        )
    )
    assert len(df) == 431
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-03-01_2023-03-25.parquet"
        )
    )
    assert len(df) == 1111


def test_chunkby_all(tmpdir):
    """Test chunkby all."""
    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=Path(tmpdir),
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(
            tmpdir,
            "2023-02-20_2023-02-25.parquet"
        )
    )
    assert len(df) == 241
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-25 00:00:00")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="teehr-") as tempdir:
        test_chunkby_location_id(tempfile.mkdtemp(dir=tempdir))
        test_chunkby_day(tempfile.mkdtemp(dir=tempdir))
        test_chunkby_week(tempfile.mkdtemp(dir=tempdir))
        test_chunkby_month(tempfile.mkdtemp(dir=tempdir))
        test_chunkby_all(tempfile.mkdtemp(dir=tempdir))
