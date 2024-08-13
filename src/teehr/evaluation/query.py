"""Query the database for evaluation purposes."""
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class Query:
    """Component class for fetching data from external sources."""

    def __init__(self, eval) -> None:
        """Initialize the Fetch class."""
        self.eval = eval

    def get_locations(
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

    def get_crosswalk(
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
