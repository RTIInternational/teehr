import pickle

import pytest
from teehr.loading.utils_nwm import build_remote_nwm_filelist


def test_remote_filelist():
    run = "analysis_assim"
    output_type = "channel_rt"
    t_minus_hours = [2]

    start_date = "2023-03-18"
    ingest_days = 1

    component_paths = build_remote_nwm_filelist(
        run,
        output_type,
        start_date,
        ingest_days,
        t_minus_hours,
    )

    test_list_path = "data/test_remote_list.pkl"
    with open(test_list_path, "rb") as pkl:
        test_list = pickle.load(pkl)

    test_list.sort()
    component_paths.sort()

    assert test_list == component_paths


if __name__ == "__main__":
    test_remote_filelist()
    pass
