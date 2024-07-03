"""Provides the teehr accessor extending pandas DataFrame objects."""
from typing import List

import pandas as pd


@pd.api.extensions.register_dataframe_accessor("teehr")
class TEEHRDataFrameAccessor:
    """Extends pandas DataFrame objects."""

    def __init__(self, pandas_obj):
        """Initialize the class."""
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        """Validate the DataFrame object columns."""
        if "primary_location_id" not in obj.columns:
            raise AttributeError("Must have 'primary_location_id'.")
        if obj.index.size == 0:
            raise AttributeError("DataFrame must have data.")

    @property
    def center(self):
        """Some property."""
        if "geometry" not in self._obj.columns:
            raise AttributeError("DataFrame must have a 'geometry' column.")
        lat = self._obj.geometry.y
        lon = self._obj.geometry.x
        return (float(lon.mean()), float(lat.mean()))

    def scatter_plot(self):
        """Create a scatter plot."""
        print("About to create a DataFrame-based scatter plot!")
        pass

    def box_whisker_plot(self):
        """Create a box and whisker plot."""
        print("About to create a DataFrame-based box and whisker plot!")
        pass

    def summarize_metrics(
        self,
        group_by: List[str] = ["primary_location_id"],
        percentiles: List[float] = [0.5],
    ) -> pd.DataFrame:
        """Summarize the DataFrame metrics."""
        summary_df = self._obj.groupby(group_by) \
            .describe(percentiles=percentiles).unstack().unstack() \
            .reset_index().rename(
                columns={'level_0': 'metric', 'level_1': 'summary'}
            )
        final_df = summary_df.pivot(
            index="metric",
            columns="summary",
            values=self._obj[group_by].values.ravel()
        )
        return final_df
