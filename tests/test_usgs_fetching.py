"""Test USGS loading."""
from datetime import datetime
from pathlib import Path
import tempfile

import pandas as pd

from teehr.evaluation.evaluation import Evaluation


def test_chunkby_location_id(tmpdir):
    """Test chunkby location id."""
    eval = Evaluation(tmpdir)
    eval.clone_template()

    eval.fetch_usgs_streamflow(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        chunk_by="location_id",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(eval.temp_dir, "02449838.parquet"))
    assert len(df) == 120
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")
    df = pd.read_parquet(Path(eval.temp_dir, "02450825.parquet"))
    assert len(df) == 119
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")


def test_chunkby_day(tmpdir):
    """Test chunkby day."""
    eval = Evaluation(tmpdir)
    eval.clone_template()

    eval.fetch_usgs_streamflow(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        chunk_by="day",
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-20.parquet")
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-21.parquet")
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-22.parquet")
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-23.parquet")
    )
    assert len(df) == 48
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-24.parquet")
    )
    assert len(df) == 47  # missing hour 17
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-25.parquet")
    )
    assert len(df) == 48


def test_chunkby_week(tmpdir):
    """Test chunk by week."""
    eval = Evaluation(tmpdir)
    eval.clone_template()

    eval.fetch_usgs_streamflow(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 3),
        chunk_by="week",
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-20_2023-02-26.parquet")
    )
    assert len(df) == 335
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-27_2023-03-03.parquet")
    )
    assert len(df) == 186


def test_chunkby_month(tmpdir):
    """Test chunk by month."""
    eval = Evaluation(tmpdir)
    eval.clone_template()

    eval.fetch_usgs_streamflow(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 25),
        chunk_by="month",
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-20_2023-02-28.parquet")
    )
    assert len(df) == 431
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-03-01_2023-03-25.parquet")
    )
    assert len(df) == 1111


def test_chunkby_all(tmpdir):
    """Test chunkby all."""
    eval = Evaluation(tmpdir)
    eval.clone_template()

    eval.fetch_usgs_streamflow(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        overwrite_output=True
    )
    df = pd.read_parquet(
        Path(eval.temp_dir, "2023-02-20_2023-02-25.parquet")
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
