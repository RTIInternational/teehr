"""Tests for retrospective NWM point loading."""
from pathlib import Path
import pandas as pd
import shutil
import pytest
from datetime import datetime
import math

from teehr.evaluation.evaluation import Evaluation

LOCATION_IDS = [7086109]
TEMP_DIR = Path("tests", "data", "temp", "nwm_retro")


@pytest.fixture(scope="session")
def temp_dir_fixture(tmp_path_factory):
    """Create a temporary directory pytest fixture."""
    temp_dir = tmp_path_factory.mktemp("retro_points")
    return temp_dir


def test_nwm20_retro_one_file(temp_dir_fixture):
    """Test NWM20 one file."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_points(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date="2000-01-01",
        end_date="2000-01-02",
        location_ids=LOCATION_IDS,
        overwrite_output=True,
    )
    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20000101_20000102.parquet")
    )
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


def test_nwm20_retro_week(temp_dir_fixture):
    """Test NWM20 one week."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_points(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 10),
        end_date=datetime(2000, 1, 16),
        location_ids=LOCATION_IDS,
        chunk_by="week",
        overwrite_output=True,
    )
    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20000110_20000116.parquet")
    )
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


def test_nwm20_retro_month(temp_dir_fixture):
    """Test NWM20 one month."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_points(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 31),
        location_ids=LOCATION_IDS,
        chunk_by="month",
        overwrite_output=True,
    )
    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20000101_20000131.parquet")
    )
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


def test_nwm20_retro_year(temp_dir_fixture):
    """Test NWM20 one year."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_points(
        nwm_version="nwm20",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 12, 31),
        location_ids=LOCATION_IDS,
        chunk_by="year",
        overwrite_output=True,
    )
    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20000101_20001231.parquet")
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


def test_nwm21_retro_one_file(temp_dir_fixture):
    """Test NWM21 one file."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_points(
        nwm_version="nwm21",
        variable_name="streamflow",
        start_date="2000-01-01",
        end_date="2000-01-02",
        location_ids=LOCATION_IDS,
        overwrite_output=True,
    )
    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20000101_20000102.parquet")
    )
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


def test_nwm30_one_file(temp_dir_fixture):
    """Test NWM30 one file."""
    eval = Evaluation(temp_dir_fixture)
    eval.clone_template()

    eval.fetch_nwm_retrospective_points(
        nwm_version="nwm30",
        variable_name="streamflow",
        start_date=datetime(2000, 1, 1),
        end_date=datetime(2000, 1, 2),
        location_ids=LOCATION_IDS,
        chunk_by=None,
        overwrite_output=True,
    )

    df = pd.read_parquet(
        Path(eval.secondary_timeseries_dir, "20000101_20000102.parquet")
    )
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
    TEMP_DIR.mkdir(exist_ok=True)
    test_nwm20_retro_one_file(TEMP_DIR)
    test_nwm20_retro_week(TEMP_DIR)
    test_nwm20_retro_month(TEMP_DIR)
    test_nwm20_retro_year(TEMP_DIR)
    test_nwm21_retro_one_file(TEMP_DIR)
    test_nwm30_one_file(TEMP_DIR)
    shutil.rmtree(TEMP_DIR)
