"""Module for querying the dataset."""
import pandas as pd
import geopandas as gpd

from teehr.querying.joined_timeseries import get_joined_timeseries


class Query:
    """Component class for querying the dataset."""

    def __init__(self, eval) -> None:
        """Initialize the Load class."""
        self.spark = eval.spark
        self.dataset_dir = eval.dataset_dir
        self.joined_timeseries_dir = eval.joined_timeseries_dir

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
