from pathlib import Path
import pandas as pd
# import pytest
from datetime import datetime

from teehr.loading.nwm_common.retrospective import nwm_retro_to_parquet

LOCATION_IDS = [7086109]
TEMP_DIR = Path("tests", "data", "temp", "retro")


def test_nwm20_retro_one_file():
    TEST_DIR = Path(TEMP_DIR, "nwm20_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date="2000-01-01",
        end_date="2000-01-02",
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "nwm20_retrospective.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm20-7086109"
    assert df["configuration"].unique()[0] == "nwm20_retrospective"


def test_nwm20_retro_day():
    TEST_DIR = Path(TEMP_DIR, "nwm20_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="day",
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "2000-01-01.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-01 23:00:00")
    # TODO: Is this meant to create files for 2-days?
    # df = pd.read_parquet(Path(TEST_DIR, "2000-01-02.parquet"))
    # assert len(df) == 24
    # assert df["value_time"].min() == pd.Timestamp("2000-01-02 00:00:00")
    # assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")


def test_nwm20_retro_location():
    TEST_DIR = Path(TEMP_DIR, "nwm20_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="location_id",
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "7086109.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm20-7086109"
    assert df["configuration"].unique()[0] == "nwm20_retrospective"


def test_nwm21_retro_one_file():
    TEST_DIR = Path(TEMP_DIR, "nwm21_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm21",
        variable_name="streamflow",
        start_date="2000-01-01",
        end_date="2000-01-02",
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "nwm21_retrospective.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm21-7086109"
    assert df["configuration"].unique()[0] == "nwm21_retrospective"


def test_nwm21_retro_day():
    TEST_DIR = Path(TEMP_DIR, "nwm21_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm21",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="day",
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "2000-01-01.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-01 23:00:00")
    # TODO: Is this meant to create files for 2-days?
    # df = pd.read_parquet(Path(TEST_DIR, "2000-01-02.parquet"))
    # assert len(df) == 24
    # assert df["value_time"].min() == pd.Timestamp("2000-01-02 00:00:00")
    # assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")


def test_nwm21_retro_location():
    TEST_DIR = Path(TEMP_DIR, "nwm21_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm21",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="location_id",
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "7086109.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm21-7086109"
    assert df["configuration"].unique()[0] == "nwm21_retrospective"


if __name__ == "__main__":
    test_nwm20_retro_one_file()
    test_nwm20_retro_day()
    test_nwm20_retro_location()
    test_nwm21_retro_one_file()
    test_nwm21_retro_day()
    test_nwm21_retro_location()
