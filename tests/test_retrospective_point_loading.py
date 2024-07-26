"""Tests for retrospective NWM point loading."""
from pathlib import Path
import pandas as pd

from datetime import datetime
import math

from teehr_v0_3.loading.nwm.retrospective_points import nwm_retro_to_parquet

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
    df = pd.read_parquet(Path(TEST_DIR, "20000101_20000102.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm20-7086109"
    assert df["configuration"].unique()[0] == "nwm20_retrospective"
    assert df.columns.to_list() == [
        "value_time",
        "location_id",
        "value",
        "measurement_unit",
        "variable_name",
        "configuration",
        "reference_time",
    ]


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
    df = pd.read_parquet(Path(TEST_DIR, "20000110_20000116.parquet"))
    assert len(df) == 168
    assert df["value_time"].min() == pd.Timestamp("2000-01-10 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-16 23:00:00")
    assert df.columns.to_list() == [
        "value_time",
        "location_id",
        "value",
        "measurement_unit",
        "variable_name",
        "configuration",
        "reference_time",
    ]


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
    df = pd.read_parquet(Path(TEST_DIR, "20000101_20000131.parquet"))
    assert len(df) == 744
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-31 23:00:00")
    assert df.columns.to_list() == [
        "value_time",
        "location_id",
        "value",
        "measurement_unit",
        "variable_name",
        "configuration",
        "reference_time",
    ]


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
        Path(TEST_DIR, "20000101_20001231.parquet")
    )
    assert len(df) == 8784
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-12-31 23:00:00")
    assert df.columns.to_list() == [
        "value_time",
        "location_id",
        "value",
        "measurement_unit",
        "variable_name",
        "configuration",
        "reference_time",
    ]


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
    df = pd.read_parquet(Path(TEST_DIR, "20000101_20000102.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm21-7086109"
    assert df["configuration"].unique()[0] == "nwm21_retrospective"
    assert df.columns.to_list() == [
        "value_time",
        "location_id",
        "value",
        "measurement_unit",
        "variable_name",
        "configuration",
        "reference_time",
    ]


def test_nwm30_one_file():
    """Test NWM30 one file."""
    TEST_DIR = Path(TEMP_DIR, "nwm30_retrospective")
    nwm_retro_to_parquet(
        nwm_version="nwm30",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        output_parquet_dir=TEST_DIR,
        chunk_by=None,
        overwrite_output=True,
    )

    df = pd.read_parquet(Path(TEST_DIR, "20000101_20000102.parquet"))
    assert len(df) == 48
    assert df["value_time"].min() == pd.Timestamp("2000-01-01 00:00:00")
    assert df["value_time"].max() == pd.Timestamp("2000-01-02 23:00:00")
    assert df["location_id"].unique()[0] == "nwm30-7086109"
    assert df["configuration"].unique()[0] == "nwm30_retrospective"
    assert df.columns.to_list() == [
        "value_time",
        "location_id",
        "value",
        "measurement_unit",
        "variable_name",
        "configuration",
        "reference_time",
    ]
    assert math.isclose(df.value.max(), 0.1799999, rel_tol=1e-4)
    assert math.isclose(df.value.min(), 0.1699999, rel_tol=1e-4)


if __name__ == "__main__":
    test_nwm20_retro_one_file()
    test_nwm20_retro_week()
    test_nwm20_retro_month()
    test_nwm20_retro_year()
    test_nwm21_retro_one_file()
    test_nwm30_one_file()
