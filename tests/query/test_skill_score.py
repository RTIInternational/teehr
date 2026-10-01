"""Tests for metric skill score calculation."""
import pandas as pd
import pytest

from teehr import DeterministicMetrics
from teehr.querying.utils import calculate_metric_skill_score

REF = "benchmark"


def _metrics_sdf(spark):
    rows = [
        ("loc_a", None, REF, 2.0),
        ("loc_a", None, "fcst_1", 1.0),
        ("loc_a", None, "fcst_2", 3.0),
        ("loc_b", "m1", REF, 0.0),
        ("loc_b", "m1", "fcst_1", 1.0),
        ("loc_c", "m1", "fcst_1", 1.0),
    ]
    return spark.createDataFrame(
        rows,
        "primary_location_id string, member string, "
        "configuration_name string, mae double",
    )


def test_skill_score_values(spark_shared_session):
    """Skill is relative to the matching reference row, NULL where undefined."""
    sdf = _metrics_sdf(spark_shared_session)
    result = calculate_metric_skill_score(
        sdf, "mae", REF, ["primary_location_id", "member", "configuration_name"]
    )

    assert result.columns == sdf.columns + ["mae_skill_score"]
    df = result.toPandas().set_index(["primary_location_id", "configuration_name"])
    skill = df["mae_skill_score"]
    # A null group key (member) still matches its reference row.
    assert skill[("loc_a", "fcst_1")] == pytest.approx(0.5)
    assert skill[("loc_a", "fcst_2")] == pytest.approx(-0.5)
    assert pd.isna(skill[("loc_a", REF)])
    # Zero reference value.
    assert pd.isna(skill[("loc_b", "fcst_1")])
    # No reference row for the group.
    assert pd.isna(skill[("loc_c", "fcst_1")])
    assert len(df) == len(sdf.collect())


def test_skill_score_group_by_configuration_only(spark_shared_session):
    """With only configuration_name in group_by, every row uses the one reference."""
    sdf = _metrics_sdf(spark_shared_session).filter("primary_location_id = 'loc_a'")
    sdf = sdf.drop("primary_location_id", "member")
    result = calculate_metric_skill_score(sdf, "mae", REF, "configuration_name")

    df = result.toPandas().set_index("configuration_name")
    assert df.loc["fcst_1", "mae_skill_score"] == pytest.approx(0.5)
    assert pd.isna(df.loc[REF, "mae_skill_score"])


@pytest.mark.module_scope_test_warehouse
def test_aggregate_with_skill_score_runs_no_spark_jobs(module_scope_test_warehouse):
    """Setting reference_configuration must keep aggregate() lazy."""
    ev = module_scope_test_warehouse
    accessor = ev.table("joined_timeseries")
    accessor.to_sdf()
    reference = accessor.to_pandas()["configuration_name"].iloc[0]

    metric = DeterministicMetrics.MeanAbsoluteError()
    metric.reference_configuration = reference

    sc = ev.spark.sparkContext
    group_id = "teehr-skill-score-laziness"
    sc.setJobGroup(group_id, "assert skill score runs no Spark jobs")
    try:
        results = accessor.aggregate(
            metrics=[metric],
            group_by=["primary_location_id", "configuration_name"],
        )
    finally:
        sc.setLocalProperty("spark.jobGroup.id", None)

    assert list(sc.statusTracker().getJobIdsForGroup(group_id)) == []
    assert "mean_absolute_error_skill_score" in results.to_pandas().columns
