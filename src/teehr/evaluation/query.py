"""Module for querying the dataset."""
import logging
from pathlib import Path
from typing import List

import pandas as pd
import geopandas as gpd
from typing import Union, List
from teehr.models.dataset.filters import (
    UnitFilter,
    VariableFilter,
    AttributeFilter,
    ConfigurationFilter,
    LocationFilter,
    LocationAttributeFilter,
    LocationCrosswalkFilter,
    TimeseriesFilter,
    JoinedTimeseriesFilter
)
from teehr.querying.field_enums import (
    UnitFields,
    VariableFields,
    AttributeFields,
    ConfigurationFields,
    LocationFields,
    LocationAttributeFields,
    LocationCrosswalkFields,
    TimeseriesFields,
    JoinedTimeseriesFields
)
from teehr.querying.table_queries import (
    get_units,
    get_variables,
    get_attributes,
    get_configurations,
    get_locations,
    get_location_attributes,
    get_location_crosswalks,
    get_timeseries,
    get_joined_timeseries
)


class Query:
    """Component class for querying the dataset."""

    def __init__(self, eval) -> None:
        """Initialize the Load class.

        ToDo: Not sure if we should set all the paths here or reference const.
        """
        self.spark = eval.spark
        self.dataset_dir = eval.dataset_dir
        self.units_dir = eval.units_dir
        self.variables_dir = eval.variables_dir
        self.attributes_dir = eval.attributes_dir
        self.configurations_dir = eval.configurations_dir
        self.locations_dir = eval.locations_dir
        self.location_attributes_dir = eval.location_attributes_dir
        self.location_crosswalks_dir = eval.location_crosswalks_dir
        self.primary_timeseries_dir = eval.primary_timeseries_dir
        self.secondary_timeseries_dir = eval.secondary_timeseries_dir
        self.joined_timeseries_dir = eval.joined_timeseries_dir

    def get_units(
        self,
        filters: Union[UnitFilter, List[UnitFilter]] = None,
        order_by: Union[UnitFields, List[UnitFields]] = None
    ) -> pd.DataFrame:
        """Get the units in the dataset."""
        return get_units(
            self.spark,
            self.units_dir,
            filters=filters,
            order_by=order_by
        )

    def get_variables(
        self,
        filters: Union[VariableFilter, List[VariableFilter]] = None,
        order_by: Union[VariableFields, List[VariableFields]] = None
    ) -> pd.DataFrame:
        """Get the variables in the dataset."""
        return get_variables(
            self.spark,
            self.variables_dir,
            filters=filters,
            order_by=order_by
        )

    def get_attributes(
        self,
        filters: Union[AttributeFilter, List[AttributeFilter]] = None,
        order_by: Union[AttributeFields, List[AttributeFields]] = None
    ) -> pd.DataFrame:
        """Get the attributes in the dataset."""
        return get_attributes(
            self.spark,
            self.attributes_dir,
            filters=filters,
            order_by=order_by
        )

    def get_configurations(
        self,
        filters: Union[
            ConfigurationFilter,
            List[ConfigurationFilter]
        ] = None,
        order_by: Union[
            ConfigurationFields,
            List[ConfigurationFields]
        ] = None
    ) -> pd.DataFrame:
        """Get the configurations in the dataset."""
        return get_configurations(
            self.spark,
            self.configurations_dir,
            filters=filters,
            order_by=order_by
        )

    def get_locations(
        self,
        filters: Union[LocationFilter, List[LocationFilter]] = None,
        order_by: Union[LocationFields, List[LocationFields]] = None
    ) -> gpd.GeoDataFrame:
        """Get the locations in the dataset."""
        return get_locations(
            self.spark,
            self.locations_dir,
            filters=filters,
            order_by=order_by
        )

    def get_location_crosswalks(
        self,
        filters: Union[
            LocationCrosswalkFilter,
            List[LocationCrosswalkFilter]
        ] = None,
        order_by: Union[
            LocationCrosswalkFields,
            List[LocationCrosswalkFields]
        ] = None
    ) -> pd.DataFrame:
        """Get the location crosswalks in the dataset."""
        return get_location_crosswalks(
            self.spark,
            self.location_crosswalks_dir,
            filters=filters,
            order_by=order_by
        )

    def get_location_attributes(
        self,
        filters: Union[
            LocationAttributeFilter,
            List[LocationAttributeFilter]
        ] = None,
        order_by: Union[
            LocationAttributeFields,
            List[LocationAttributeFields]
        ] = None
    ) -> pd.DataFrame:
        """Get the location attributes in the dataset."""
        return get_location_attributes(
            self.spark,
            self.location_attributes_dir,
            filters=filters,
            order_by=order_by
        )

    def get_primary_timeseries(
        self,
        filters: Union[TimeseriesFilter, List[TimeseriesFilter]] = None,
        order_by: Union[TimeseriesFields, List[TimeseriesFields]] = None
    ) -> pd.DataFrame:
        """Get the primary timeseries in the dataset."""
        return get_timeseries(
            self.spark,
            self.primary_timeseries_dir,
            filters=filters,
            order_by=order_by
        )

    def get_secondary_timeseries(
        self,
        filters: Union[TimeseriesFilter, List[TimeseriesFilter]] = None,
        order_by: Union[TimeseriesFields, List[TimeseriesFields]] = None
    ) -> pd.DataFrame:
        """Get the secondary timeseries in the dataset."""
        return get_timeseries(
            self.spark,
            self.secondary_timeseries_dir,
            filters=filters,
            order_by=order_by
        )

    def get_joined_timeseries(
        self,
        filters: Union[
            JoinedTimeseriesFilter,
            List[JoinedTimeseriesFilter]
        ] = None,
        order_by: Union[
            JoinedTimeseriesFields,
            List[JoinedTimeseriesFields]
        ] = None
    ) -> pd.DataFrame:
        """Get the joined timeseries in the dataset."""
        return get_joined_timeseries(
            self.spark,
            self.joined_timeseries_dir,
            filters=filters,
            order_by=order_by
        )

    def get_metrics(self):
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