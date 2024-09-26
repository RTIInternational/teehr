"""Test fetching and loading data into the dataset."""
from pathlib import Path
from datetime import datetime
import tempfile

from teehr import Evaluation
import pandas as pd
import numpy as np
import pytest


TEST_STUDY_DATA_DIR = Path("tests", "data", "test_study")
GEO_GAGES_FILEPATH = Path(
    TEST_STUDY_DATA_DIR,
    "geo",
    "usgs_point_geometry.test.parquet"
)
CROSSWALK_FILEPATH = Path(
    TEST_STUDY_DATA_DIR,
    "geo",
    "usgs_nwm30_crosswalk.test.parquet"
)
ZONAL_WEIGHTS = Path(
    "tests", "data", "nwm22", "onehuc10_weights_retro.parquet"
)
ZONAL_LOCATIONS = Path(
    TEST_STUDY_DATA_DIR,
    "geo",
    "one_huc10_conus_1016000606.parquet"
)


def test_fetch_and_load_nwm_retro_points(tmpdir):
    """Test the NWM retro point fetch and load."""
    eval = Evaluation(dir_path=tmpdir)
    eval.enable_logging()
    eval.clone_template()

    _ = eval.locations.load_spatial(in_path=GEO_GAGES_FILEPATH).to_geopandas()

    eval.fetch.usgs_streamflow(
        start_date=datetime(2022, 2, 22),
        end_date=datetime(2022, 2, 23)
    )

    _ = eval.primary_timeseries.to_pandas()

    eval.location_crosswalks.load_parquet(
        in_path=CROSSWALK_FILEPATH
    )

    eval.fetch.nwm_retrospective_points(
        nwm_version="nwm30",
        variable_name="streamflow",
        start_date=datetime(2022, 2, 22),
        end_date=datetime(2022, 2, 23)
    )

    ts_df = eval.secondary_timeseries.to_pandas()

    assert isinstance(ts_df, pd.DataFrame)
    assert ts_df.columns.tolist() == [
            "reference_time",
            "value_time",
            "configuration_name",
            "unit_name",
            "variable_name",
            "value",
            "location_id"
            ]
    assert ts_df.unit_name.iloc[0] == "m^3/s"
    assert ts_df.variable_name.iloc[0] == "streamflow_hourly_inst"
    assert ts_df.value.sum() == np.float32(7319.99)
    assert ts_df.value_time.min() == pd.Timestamp("2022-02-22 00:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2022-02-23 23:00:00")


def test_fetch_and_load_nwm_retro_grids(tmpdir):
    """Test the NWM retro grid fetch and load."""
    eval = Evaluation(dir_path=tmpdir)
    eval.enable_logging()
    eval.clone_template()

    # Add locations corresponding to weights file.
    eval.locations.load_spatial(in_path=ZONAL_LOCATIONS)

    eval.fetch.nwm_retrospective_grids(
        nwm_version="nwm30",
        variable_name="RAINRATE",
        zonal_weights_filepath=ZONAL_WEIGHTS,
        start_date="2008-05-23 09:00",
        end_date="2008-05-23 10:00",
        location_id_prefix="huc10"
    )
    ts_df = eval.primary_timeseries.to_pandas()

    assert isinstance(ts_df, pd.DataFrame)
    assert ts_df.columns.tolist() == [
            "reference_time",
            "value_time",
            "configuration_name",
            "unit_name",
            "variable_name",
            "value",
            "location_id"
            ]
    assert ts_df.unit_name.iloc[0] == "mm/s"
    assert ts_df.variable_name.iloc[0] == "rainfall_hourly_rate"
    assert ts_df.value.sum() == np.float32(0.00028747512)
    assert ts_df.value_time.min() == pd.Timestamp("2008-05-23 09:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2008-05-23 23:00:00")


def test_fetch_and_load_nwm_forecast_points(tmpdir):
    """Test the NWM forecast point fetch and load."""
    eval = Evaluation(dir_path=tmpdir)
    eval.enable_logging()
    eval.clone_template()

    eval.locations.load_spatial(in_path=GEO_GAGES_FILEPATH)

    eval.location_crosswalks.load_parquet(
        in_path=CROSSWALK_FILEPATH
    )

    eval.fetch.nwm_forecast_points(
        configuration="analysis_assim",
        output_type="channel_rt",
        variable_name="streamflow",
        start_date=datetime(2024, 2, 22),
        ingest_days=1,
        nwm_version="nwm30",
        t_minus_hours=[0],
        process_by_z_hour=False
    )

    ts_df = eval.secondary_timeseries.to_pandas()

    assert isinstance(ts_df, pd.DataFrame)
    assert ts_df.columns.tolist() == [
            "reference_time",
            "value_time",
            "configuration_name",
            "unit_name",
            "variable_name",
            "value",
            "location_id"
            ]
    assert ts_df.unit_name.iloc[0] == "m^3/s"
    assert ts_df.variable_name.iloc[0] == "streamflow_hourly_inst"
    assert ts_df.value.sum() == np.float32(658.14)
    assert ts_df.value_time.min() == pd.Timestamp("2024-02-22 00:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2024-02-22 23:00:00")


@pytest.mark.skip(reason="This takes forever!")
def test_fetch_and_load_nwm_forecast_grids(tmpdir):
    """Test the NWM forecast grids fetch and load."""
    eval = Evaluation(dir_path=tmpdir)
    eval.enable_logging()
    eval.clone_template()

    eval.locations.load_spatial(in_path=ZONAL_LOCATIONS)

    eval.fetch.nwm_forecast_grids(
        configuration="forcing_analysis_assim",
        output_type="forcing",
        variable_name="RAINRATE",
        start_date=datetime(2024, 2, 22),
        ingest_days=1,
        zonal_weights_filepath=ZONAL_WEIGHTS,
        nwm_version="nwm30",
        t_minus_hours=[0],
        location_id_prefix="huc10"
    )

    ts_df = eval.primary_timeseries.to_pandas()

    assert isinstance(ts_df, pd.DataFrame)
    assert ts_df.columns.tolist() == [
            "reference_time",
            "value_time",
            "configuration_name",
            "unit_name",
            "variable_name",
            "value",
            "location_id"
            ]
    assert ts_df.unit_name.iloc[0] == "mm/s"
    assert ts_df.variable_name.iloc[0] == "rainfall_hourly_rate"
    assert ts_df.value.sum() == np.float32(0.0)
    assert ts_df.value_time.min() == pd.Timestamp("2024-02-22 00:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2024-02-22 00:00:00")
    file_list = list(
        Path(
            tmpdir,
            "dataset",
            "primary_timeseries",
            "nwm30_forcing_analysis_assim",
            "rainfall_hourly_rate"
            ).glob("*.parquet")
    )
    assert len(file_list) == 24


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_fetch_and_load_nwm_retro_points(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_fetch_and_load_nwm_retro_grids(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_fetch_and_load_nwm_forecast_points(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        # # Warning: This one is slow.
        # test_fetch_and_load_nwm_forecast_grids(
        #     tempfile.mkdtemp(
        #         prefix="4-",
        #         dir=tempdir
        #     )
        # )
