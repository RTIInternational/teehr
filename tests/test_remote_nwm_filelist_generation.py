import pandas as pd
from pathlib import Path
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

    test_list_path = Path("tests", "data", "test_remote_list.csv")
    test_df = pd.read_csv(test_list_path)
    test_list = test_df["filename"].to_list()

    test_list.sort()
    component_paths.sort()

    assert test_list == component_paths


if __name__ == "__main__":
    test_remote_filelist()
    pass
