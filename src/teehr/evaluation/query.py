"""Query the database for evaluation purposes."""
import logging
from pathlib import Path
from typing import List

import duckdb

logger = logging.getLogger(__name__)


class Query:
    """Component class for fetching data from external sources."""

    def __init__(self, eval) -> None:
        """Initialize the Fetch class."""
        self.eval = eval

    def get_locations_table(
        self,
        pattern: str = "**/*.parquet"
    ):
        """Get locations from the dataset."""
        # TODO: Add filters and order-by clauses.
        conn = duckdb.connect()

        in_path = Path(self.eval.locations_dir)

        if in_path.is_dir():
            if len(list(in_path.glob(pattern))) == 0:
                logger.info(f"No Parquet files in {in_path}/{pattern}.")
            in_path = str(in_path) + pattern

        logger.debug(f"Get locations from {in_path}.")

        return conn.sql(f"""
            INSTALL spatial;
            LOAD spatial;
            SELECT id, name, ST_GeomFromWKB(geometry) as geometry
            FROM read_parquet('{in_path}');
        """).to_df()

    def get_crosswalk_table(
        self,
        pattern: str = "**/*.parquet"
    ):
        """Get cross walk table from the dataset."""
        # TODO: Add filters and order-by clauses.
        conn = duckdb.connect()

        in_path = Path(self.eval.locations_crosswalk_dir)

        if in_path.is_dir():
            if len(list(in_path.glob(pattern))) == 0:
                logger.info(f"No Parquet files in {in_path}/{pattern}.")
            in_path = str(in_path) + pattern

        logger.debug(f"Get crosswalk from {in_path}.")

        return conn.sql(f"""
            SELECT primary_location_id, secondary_location_id
            FROM read_parquet('{in_path}');
        """).to_df()

    def get_secondary_location_ids(
        self,
        primary_location_ids: List[str],
        pattern: str = "**/*.parquet"
    ):
        """Get secondary location ids from the dataset."""
        # TODO: Add filters and order-by clauses.
        conn = duckdb.connect()

        in_path = Path(self.eval.locations_crosswalk_dir)

        if in_path.is_dir():
            if len(list(in_path.glob(pattern))) == 0:
                logger.info(f"No Parquet files in {in_path}/{pattern}.")
            in_path = str(in_path) + pattern

        logger.debug("Getting secondary IDs from crosswalk.")

        return conn.sql(f"""
            SELECT secondary_location_id
            FROM read_parquet('{in_path}')
            WHERE primary_location_id IN {tuple(primary_location_ids)};
        """).to_df()
