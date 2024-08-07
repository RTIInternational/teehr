"""Tests for real-time NWM point forecast loading."""
# from pathlib import Path
# import pandas as pd

from unittest.mock import MagicMock, Mock
from datetime import datetime
import tempfile

from teehr.evaluation.evaluation import Evaluation

LOCATION_IDS = [7086109]


def test_nwm30_point_forecast_fetching(tmpdir):
    """Test NWM30 point forecast fetching."""
    eval = Evaluation(tmpdir)
    eval.clone_template()

    # m = MagicMock(eval.fetch_nwm_forecast_points)

    # mock = Mock()

    # eval.fetch_nwm_forecast_points = MagicMock(name="fetch_nwm_forecast_points")
    # eval.fetch_nwm_forecast_points

    eval.fetch_nwm_forecast_points(
        nwm_version="nwm30",
        configuration="short_range",
        output_type="channel_rt",
        variable_name="streamflow",
        start_date=datetime(2023, 11, 1),
        ingest_days=1,
        location_ids=LOCATION_IDS,
        overwrite_output=True,
    )

    pass


def test_nwm30_grid_forecast_fetching(tmpdir):
    """Test NWM30 grid forecast fetching."""
    eval = Evaluation(tmpdir)
    eval.clone_template()

    eval.fetch_nwm_forecast_grids(
        nwm_version="xxxx",
        configuration="short_range",
        output_type="channel_rt",
        variable_name="streamflow",
        start_date=datetime(2023, 11, 1),
        ingest_days=1,
        zonal_weights_filepath="test/test/test.parquet",
        overwrite_output=True,
    )
    pass


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="teehr-") as tempdir:
        test_nwm30_point_forecast_fetching(
            tempfile.mkdtemp(dir=tempdir)
        )
        # test_nwm30_grid_forecast_fetching(
        #     tempfile.mkdtemp(dir=tempdir)
        # )