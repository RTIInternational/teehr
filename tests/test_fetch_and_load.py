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
    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    ev.locations.load_spatial(in_path=GEO_GAGES_FILEPATH)

    ev.fetch.usgs_streamflow(
        start_date=datetime(2022, 2, 22),
        end_date=datetime(2022, 2, 23)
    )

    # Make sure second fetch succeeds.
    ev.fetch.usgs_streamflow(
        start_date=datetime(2022, 2, 24),
        end_date=datetime(2022, 2, 25)
    )

    pts_df = ev.primary_timeseries.to_pandas()

    ev.location_crosswalks.load_parquet(
        in_path=CROSSWALK_FILEPATH
    )

    ev.fetch.nwm_retrospective_points(
        nwm_version="nwm30",
        variable_name="streamflow",
        start_date=datetime(2022, 2, 22),
        end_date=datetime(2022, 2, 23)
    )
    ts_df = ev.secondary_timeseries.to_pandas()

    # Make sure second fetch succeeds.
    ev.fetch.nwm_retrospective_points(
        nwm_version="nwm30",
        variable_name="streamflow",
        start_date=datetime(2022, 2, 24),
        end_date=datetime(2022, 2, 25)
    )

    assert pts_df.value_time.min() == pd.Timestamp("2022-02-22 00:00:00")
    assert pts_df.value_time.max() == pd.Timestamp("2022-02-25 00:00:00")
    assert isinstance(ts_df, pd.DataFrame)
    assert set(ts_df.columns.tolist()) == set([
            "reference_time",
            "value_time",
            "value",
            "unit_name",
            "location_id",
            "configuration_name",
            "variable_name",
            "member"
            ])
    assert ts_df.unit_name.iloc[0] == "m^3/s"
    assert np.isclose(ts_df.value.sum(), np.float32(7319.99))
    assert ts_df.value_time.min() == pd.Timestamp("2022-02-22 00:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2022-02-23 23:00:00")


def test_fetch_and_load_nwm_retro_grids(tmpdir):
    """Test the NWM retro grid fetch and load."""
    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    # Add locations corresponding to weights file.
    ev.locations.load_spatial(in_path=ZONAL_LOCATIONS)

    ev.fetch.nwm_retrospective_grids(
        nwm_version="nwm30",
        variable_name="RAINRATE",
        zonal_weights_filepath=ZONAL_WEIGHTS,
        start_date="2008-05-23 09:00",
        end_date="2008-05-23 10:00",
        location_id_prefix="huc10"
    )
    ts_df = ev.primary_timeseries.to_pandas()

    assert isinstance(ts_df, pd.DataFrame)
    assert set(ts_df.columns.tolist()) == set([
            "reference_time",
            "value_time",
            "value",
            "unit_name",
            "location_id",
            "configuration_name",
            "variable_name"
            ])
    assert ts_df.unit_name.iloc[0] == "mm/s"
    assert np.isclose(ts_df.value.sum(), np.float32(0.00028747512))
    assert ts_df.value_time.min() == pd.Timestamp("2008-05-23 09:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2008-05-23 23:00:00")


def test_fetch_and_load_nwm_operational_points(tmpdir):
    """Test the NWM operational point fetch and load."""
    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    ev.locations.load_spatial(in_path=GEO_GAGES_FILEPATH)

    ev.location_crosswalks.load_parquet(
        in_path=CROSSWALK_FILEPATH
    )

    ev.fetch.nwm_operational_points(
        nwm_configuration="analysis_assim",
        output_type="channel_rt",
        variable_name="streamflow",
        start_date=datetime(2024, 2, 22),
        ingest_days=1,
        nwm_version="nwm30",
        prioritize_analysis_valid_time=True,
        t_minus_hours=[0],
        process_by_z_hour=False,
        starting_z_hour=3,
        ending_z_hour=20
    )
    ts_df = ev.secondary_timeseries.to_pandas()

    assert isinstance(ts_df, pd.DataFrame)
    assert set(ts_df.columns.tolist()) == set([
            "reference_time",
            "value_time",
            "value",
            "unit_name",
            "location_id",
            "configuration_name",
            "variable_name",
            "member"
            ])
    assert ts_df.unit_name.iloc[0] == "m^3/s"
    assert np.isclose(ts_df.value.sum(), np.float32(492.21))
    assert ts_df.value_time.min() == pd.Timestamp("2024-02-22 03:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2024-02-22 20:00:00")


@pytest.mark.skip(reason="This takes forever!")
def test_fetch_and_load_nwm_operational_grids(tmpdir):
    """Test the NWM forecast grids fetch and load."""
    ev = Evaluation(dir_path=tmpdir)
    ev.enable_logging()
    ev.clone_template()

    ev.locations.load_spatial(in_path=ZONAL_LOCATIONS)

    ev.fetch.nwm_operational_grids(
        nwm_configuration="forcing_analysis_assim",
        output_type="forcing",
        variable_name="RAINRATE",
        start_date=datetime(2024, 2, 22),
        ingest_days=1,
        zonal_weights_filepath=ZONAL_WEIGHTS,
        nwm_version="nwm30",
        prioritize_analysis_valid_time=True,
        t_minus_hours=[0],
        location_id_prefix="huc10",
        starting_z_hour=2,
        ending_z_hour=22
    )
    ts_df = ev.primary_timeseries.to_pandas()

    assert isinstance(ts_df, pd.DataFrame)
    assert set(ts_df.columns.tolist()) == set([
            "reference_time",
            "value_time",
            "value",
            "unit_name",
            "location_id",
            "configuration_name",
            "variable_name"
            ])
    assert ts_df.unit_name.iloc[0] == "mm/s"
    assert np.isclose(ts_df.value.sum(), np.float32(0.0))
    assert ts_df.value_time.min() == pd.Timestamp("2024-02-22 02:00:00")
    assert ts_df.value_time.max() == pd.Timestamp("2024-02-22 22:00:00")
    file_list = list(
        Path(
            tmpdir,
            "dataset",
            "primary_timeseries",
            "configuration_name=nwm30_forcing_analysis_assim",
            "variable_name=rainfall_hourly_rate"
            ).glob("*.parquet")
    )
    assert len(file_list) == 7


if __name__ == "__main__":

    from dask.distributed import Client
    client = Client()

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
        test_fetch_and_load_nwm_operational_points(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        # Warning: This one is slow.
        test_fetch_and_load_nwm_operational_grids(
            tempfile.mkdtemp(
                prefix="4-",
                dir=tempdir
            )
        )
