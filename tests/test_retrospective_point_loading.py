"""Tests for retrospective NWM point loading."""
from pathlib import Path
import pandas as pd

from datetime import datetime

from teehr.loading.nwm.retrospective_points import nwm_retro_to_parquet

LOCATION_IDS = [7086109]
TEMP_DIR = Path("tests", "data", "temp", "retro")


def test_nwm20_retro_one_file():
    """Test NWM20 one file."""
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
    df = pd.read_parquet(Path(TEST_DIR, "2000010100Z_2000010223Z.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm20-7086109"
    assert df["configuration"].unique()[0] == "nwm20_retrospective"


def test_nwm20_retro_day():
    """Test NWM20 one day."""
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
    df = pd.read_parquet(Path(TEST_DIR, "20000101Z.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-01 23:00:00")
    df = pd.read_parquet(Path(TEST_DIR, "20000102Z.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-02 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")


def test_nwm20_retro_week():
    """Test NWM20 one week."""
    TEST_DIR = Path(TEMP_DIR, "nwm20_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 10),
        end_date=datetime(2000, 1, 16),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="week",
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "20000110Z_20000116Z.parquet"))
    assert len(df) == 168
    assert df["value_time"].min() == pd.Timestamp("2000-01-10 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-16 23:00:00")
    pass


def test_nwm20_retro_month():
    """Test NWM20 one month."""
    TEST_DIR = Path(TEMP_DIR, "nwm20_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 31),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="month",
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "20000101Z_20000131Z.parquet"))
    assert len(df) == 744
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-31 23:00:00")
    pass


def test_nwm20_retro_year():
    """Test NWM20 one year."""
    TEST_DIR = Path(TEMP_DIR, "nwm20_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 12, 31),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="year",
        overwrite_output=True,
    )
    df = pd.read_parquet(
        Path(TEST_DIR, "20000101Z_20001231Z.parquet")
    )
    assert len(df) == 8784
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-12-31 23:00:00")
    pass


def test_nwm20_retro_location():
    """Test NWM20 by location."""
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
    df = pd.read_parquet(
        Path(TEST_DIR, "7086109_2000010100Z_2000010223Z.parquet")
    )
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm20-7086109"
    assert df["configuration"].unique()[0] == "nwm20_retrospective"


def test_nwm21_retro_one_file():
    """Test NWM21 one file."""
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
    df = pd.read_parquet(Path(TEST_DIR, "2000010100Z_2000010223Z.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm21-7086109"
    assert df["configuration"].unique()[0] == "nwm21_retrospective"


def test_nwm21_retro_day():
    """Test NWM21 one day."""
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
    df = pd.read_parquet(Path(TEST_DIR, "20000101Z.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-01 23:00:00")
    df = pd.read_parquet(Path(TEST_DIR, "20000102Z.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-02 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")


def test_nwm21_retro_location():
    """Test NWM21 by location."""
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
    df = pd.read_parquet(
        Path(TEST_DIR, "7086109_2000010100Z_2000010223Z.parquet")
    )
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm21-7086109"
    assert df["configuration"].unique()[0] == "nwm21_retrospective"


def test_nwm30_retro_day():
    """Test NWM30 one day."""
    TEST_DIR = Path(TEMP_DIR, "nwm30_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm30",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by="day",
        overwrite_output=True,
    )
    df = pd.read_parquet(Path(TEST_DIR, "20000101Z.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-01 23:00:00")
    df = pd.read_parquet(Path(TEST_DIR, "20000102Z.parquet"))
    assert len(df) == 24
    assert df["value_time"].min() == pd.Timestamp("2000-01-02 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")


if __name__ == "__main__":
    # test_nwm20_retro_one_file()
    # test_nwm20_retro_day()
    # test_nwm20_retro_week()
    # test_nwm20_retro_month()
    # test_nwm20_retro_year()
    # test_nwm20_retro_location()
    test_nwm21_retro_one_file()
    # test_nwm21_retro_day()
    # test_nwm21_retro_location()
    # test_nwm30_retro_day()
