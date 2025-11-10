"""Test script for debugging evaluation data."""
from teehr import Evaluation
from pathlib import Path
from teehr import Metrics as metrics
from teehr.models.table_enums import (
    JoinedTimeseriesFields
)

# Set a path to the directory where the evaluation will be created
TEST_STUDY_DIR = Path(Path().home(), "temp", "real_study")
TEST_STUDY_DIR.mkdir(parents=True, exist_ok=True)

# Create an Evaluation object
ev = Evaluation(dir_path=TEST_STUDY_DIR)

# Enable logging
ev.enable_logging()

fields_list = ev.joined_timeseries.fields()
fields_list.append("relative_bias")

jtf = JoinedTimeseriesFields(
    "JoinedTimeseriesFields",
    {field: field for field in fields_list}
)

# This does not work.
df = (
    ev.metrics.query(
        order_by=["primary_location_id", "month"],
        group_by=["primary_location_id", "month"],
        include_metrics=[
            metrics.KlingGuptaEfficiency(),
            metrics.NashSutcliffeEfficiency(),
            metrics.RelativeBias()
        ]
    )
    .query(
        order_by=["primary_location_id"],
        group_by=["primary_location_id"],
        include_metrics=[
            metrics.PrimaryAverage(
                input_field_names=[jtf.relative_bias],
            )
        ]
    )
    .to_pandas()
)
print(df)