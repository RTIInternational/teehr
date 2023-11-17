import pandas as pd
from pathlib import Path
from teehr.loading.nwm_common.utils_nwm import build_remote_nwm_filelist
from teehr.loading.nwm22.const_nwm import NWM22_ANALYSIS_CONFIG


def test_remote_filelist():
    run = "analysis_assim"
    output_type = "channel_rt"
    t_minus_hours = [2]

    start_date = "2023-03-18"
    ingest_days = 1

    component_paths = build_remote_nwm_filelist(
        configuration=run,
        output_type=output_type,
        start_dt=start_date,
        ingest_days=ingest_days,
        analysis_config_dict=NWM22_ANALYSIS_CONFIG,
        t_minus_hours=t_minus_hours,
        ignore_missing_file=False
    )

    test_list_path = Path("tests", "data", "nwm22", "test_remote_list.csv")
    test_df = pd.read_csv(test_list_path)
    test_list = test_df["filename"].to_list()

    test_list.sort()
    component_paths.sort()

    assert test_list == component_paths


if __name__ == "__main__":
    test_remote_filelist()
    pass