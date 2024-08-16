"""Module for querying the dataset."""
import logging
from pathlib import Path
from typing import List

import pandas as pd
import geopandas as gpd
import duckdb

from teehr.querying.joined_timeseries import get_joined_timeseries

logger = logging.getLogger(__name__)


class Query:
    """Component class for querying the dataset."""

    def __init__(self, eval) -> None:
        """Initialize the Load class."""
        self.spark = eval.spark
        self.dataset_dir = eval.dataset_dir
        self.joined_timeseries_dir = eval.joined_timeseries_dir
        self.eval = eval

    def locations(self) -> gpd.GeoDataFrame:
        """Get the locations in the dataset."""
        pass

    def primary_timeseries(self) -> pd.DataFrame:
        """Get the primary timeseries in the dataset."""
        pass

    def secondary_timeseries(self) -> pd.DataFrame:
        """Get the secondary timeseries in the dataset."""
        pass

    def location_crosswalks(self) -> pd.DataFrame:
        """Get the location crosswalks in the dataset."""
        pass

    def location_attributes(self) -> pd.DataFrame:
        """Get the location attributes in the dataset."""
        pass

    def joined_timeseries(self) -> pd.DataFrame:
        """Get the joined timeseries in the dataset."""
        return get_joined_timeseries(
            self.spark,
            self.joined_timeseries_dir
        )

    def metrics(self):
        """Get the metrics in the dataset."""
        pass

# TEMP:
    def get_locations_table(
        self,
        pattern: str = "**/*.parquet"
    ):
        """Get locations from the dataset."""
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