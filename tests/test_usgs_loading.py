from datetime import datetime
from pathlib import Path

import pandas as pd

from teehr.loading.usgs.usgs import usgs_to_parquet

TEMP_DIR = Path("tests", "data", "temp", "usgs")


def test_chunkby_location_id():

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=TEMP_DIR,
        chunk_by="location_id",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(TEMP_DIR, "02449838.parquet"))
    assert len(df) == 120
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")
    df = pd.read_parquet(Path(TEMP_DIR, "02450825.parquet"))
    assert len(df) == 119
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")


def test_chunkby_day():

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=TEMP_DIR,
        chunk_by="day",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-20.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-21.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-22.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-23.parquet"))
    assert len(df) == 48
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-24.parquet"))
    assert len(df) == 47  # missing hour 17
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-25.parquet"))
    assert len(df) == 48


def test_chunkby_week():

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 3),
        output_parquet_dir=TEMP_DIR,
        chunk_by="week",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-20_2023-02-25.parquet"))
    assert len(df) == 287
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-26_2023-03-03.parquet"))
    assert len(df) == 266


def test_chunkby_month():

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 3, 25),
        output_parquet_dir=TEMP_DIR,
        chunk_by="month",
        overwrite_output=True
    )
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-20_2023-02-27.parquet"))
    assert len(df) == 383
    df = pd.read_parquet(Path(TEMP_DIR, "2023-02-28_2023-03-25.parquet"))
    assert len(df) == 1188  # missing final hour 23


def test_chunkby_all():

    usgs_to_parquet(
        sites=[
            "02449838",
            "02450825"
        ],
        start_date=datetime(2023, 2, 20),
        end_date=datetime(2023, 2, 25),
        output_parquet_dir=TEMP_DIR,
        overwrite_output=True
    )
    df = pd.read_parquet(Path(TEMP_DIR, "usgs.parquet"))
    assert len(df) == 239
    assert df["value_time"].min() == pd.Timestamp("2023-02-20 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2023-02-24 23:00:00")


if __name__ == "__main__":
    test_chunkby_location_id()
    test_chunkby_day()
    test_chunkby_week()
    test_chunkby_month()
    test_chunkby_all()
