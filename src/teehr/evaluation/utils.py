"""Utility functions for the evaluation class."""
import logging
from pathlib import Path
from enum import Enum
from typing import Union

import duckdb

logger = logging.getLogger(__name__)


def _get_joined_timeseries_fields(
    joined_timeseries_dir: Union[Path, str]
) -> Enum:
    """Get the field names from the joined timeseries table."""
    if len(list(Path(joined_timeseries_dir).glob("*.parquet"))) == 0:
        logger.error(f"No parquet files in {joined_timeseries_dir}.")
        raise FileNotFoundError
    else:
        logger.info(f"Reading fields from {joined_timeseries_dir}.")
        qry = f"""
        DESCRIBE
        SELECT
            *
        FROM
            read_parquet(
                '{str(Path(joined_timeseries_dir, "*.parquet"))}'
            )
        ;"""
        fields_list = duckdb.sql(qry).df().column_name.tolist()
        return Enum("Fields", {field: field for field in fields_list})
