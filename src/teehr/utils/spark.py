"""Spark helper utilities."""

from __future__ import annotations

from functools import reduce
from operator import and_
from typing import Sequence

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def null_safe_join_on_columns(
    left_df: DataFrame,
    right_df: DataFrame,
    join_columns: Sequence[str],
    how: str = "inner",
    left_alias: str = "left",
    right_alias: str = "right",
) -> DataFrame:
    """Join two Spark DataFrames using null-safe equality for key columns.

    The output includes one coalesced copy of each join key, all non-key
    columns from ``left_df``, and all non-key columns from ``right_df``.
    """
    join_columns = list(join_columns)

    left_non_key_cols = [c for c in left_df.columns if c not in join_columns]
    right_non_key_cols = [c for c in right_df.columns if c not in join_columns]

    duplicate_non_keys = sorted(set(left_non_key_cols).intersection(right_non_key_cols))
    if duplicate_non_keys:
        dup_list = ", ".join(duplicate_non_keys)
        raise ValueError(
            "null_safe_join_on_columns requires unique non-key column names "
            f"across both inputs, but found duplicates: {dup_list}."
        )

    join_condition = reduce(
        and_,
        [
            F.col(f"{left_alias}.{col_name}").eqNullSafe(
                F.col(f"{right_alias}.{col_name}")
            )
            for col_name in join_columns
        ],
        F.lit(True),
    )

    joined_df = left_df.alias(left_alias).join(
        right_df.alias(right_alias),
        on=join_condition,
        how=how,
    )

    key_cols = [
        F.coalesce(
            F.col(f"{left_alias}.{col_name}"),
            F.col(f"{right_alias}.{col_name}"),
        ).alias(col_name)
        for col_name in join_columns
    ]
    left_cols = [F.col(f"{left_alias}.{col_name}") for col_name in left_non_key_cols]
    right_cols = [F.col(f"{right_alias}.{col_name}") for col_name in right_non_key_cols]

    return joined_df.select(*key_cols, *left_cols, *right_cols)
