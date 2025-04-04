"""A utility for rewriting Evaluations with reference time as a partition."""
from pathlib import Path
from typing import List
import pyspark.sql as ps
import shutil

import teehr
from teehr.utils.utils import remove_dir_if_exists

PARTITION_BY = [
    "configuration_name",
    "variable_name",
    "reference_time"
]
KWARGS = {
    "header": "true",
}


def _check_for_null_reference_time(partition_by: List[str], sdf: ps.DataFrame):
    """Check if the reference time column is all null."""
    if "reference_time" in sdf.columns:
        if len(sdf.filter(sdf.reference_time.isNotNull()).collect()) == 0:
            if "reference_time" in partition_by:
                partition_by.remove("reference_time")
    return partition_by


def rewrite_secondary_timeseries(
    ev: teehr.Evaluation,
    partition_by: List[str] = PARTITION_BY
):
    """Rewrite secondary timeseries partitioning on reference time."""
    secondary_sdf = ev.secondary_timeseries.to_sdf()

    partition_by = _check_for_null_reference_time(
        partition_by,
        secondary_sdf
    )
    temp_path = Path(
        ev.cache_dir,
        "temp_rewrite",
        "secondary_timeseries"
    )
    (
        secondary_sdf.
        write.
        partitionBy(partition_by).
        format("parquet").
        mode("overwrite").
        options(**KWARGS).
        save(str(temp_path))
    )
    # Copy in temp dir then remove
    remove_dir_if_exists(ev.secondary_timeseries.dir)
    shutil.copytree(
        str(temp_path),
        str(ev.secondary_timeseries.dir),
        dirs_exist_ok=True
    )
    remove_dir_if_exists(Path(ev.cache_dir, "temp_rewrite"))


def rewrite_primary_timeseries(
    ev: teehr.Evaluation,
    partition_by: List[str] = PARTITION_BY
):
    """Rewrite primary timeseries partitioning on reference time."""
    primary_sdf = ev.primary_timeseries.to_sdf()

    partition_by = _check_for_null_reference_time(
        partition_by,
        primary_sdf
    )
    temp_path = Path(
        ev.cache_dir,
        "temp_rewrite",
        "primary_timeseries"
    )
    (
        primary_sdf.
        write.
        partitionBy(partition_by).
        format("parquet").
        mode("overwrite").
        options(**KWARGS).
        save(str(temp_path))
    )
    # Copy in temp dir then remove
    remove_dir_if_exists(ev.primary_timeseries.dir)
    shutil.copytree(
        str(temp_path),
        str(ev.primary_timeseries.dir),
        dirs_exist_ok=True
    )
    remove_dir_if_exists(Path(ev.cache_dir, "temp_rewrite"))


def rewrite_joined_timeseries(
    ev: teehr.Evaluation,
    partition_by: List[str] = PARTITION_BY
):
    """Rewrite joined timeseries partitioning on reference time."""
    joined_sdf = ev.joined_timeseries.to_sdf()

    partition_by = _check_for_null_reference_time(
        partition_by,
        joined_sdf
    )
    temp_path = Path(
        ev.cache_dir,
        "temp_rewrite",
        "joined_timeseries"
    )
    (
        joined_sdf.
        write.
        partitionBy(partition_by).
        format("parquet").
        mode("overwrite").
        options(**KWARGS).
        save(str(temp_path))
    )
    # Copy in temp dir then remove
    remove_dir_if_exists(ev.joined_timeseries.dir)
    shutil.copytree(
        str(temp_path),
        str(ev.joined_timeseries.dir),
        dirs_exist_ok=True
    )
    remove_dir_if_exists(Path(ev.cache_dir, "temp_rewrite"))


if __name__ == "__main__":

    # Define the path to your evaluation directory
    test_eval_dir = Path("/mnt/data/ciroh/teehr/test_stuff/temp_repartition_ev/temp_ev")

    # Create an Evaluation object and create the directory
    ev = teehr.Evaluation(dir_path=test_eval_dir)

    # Re-write the primary timeseries with reference time as a partition,
    # if it is not null.
    rewrite_primary_timeseries(ev)

    # Re-write the secondary timeseries with reference time as a partition,
    # if it is not null.
    rewrite_secondary_timeseries(ev)

    # Re-write the joined timeseries with reference time as a partition,
    # if it is not null.
    rewrite_joined_timeseries(ev)

    pass
