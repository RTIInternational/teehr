"""Provides the teehr metrics accessor extending pandas DataFrames."""
from typing import List

import pandas as pd


try:
    # Currently we have two different versions of the accessor both
    # named "teehr". This is a workaround to avoid the warning of
    # overwriting an existing accessor. Should we consider renaming?
    del pd.DataFrame.teehr
except AttributeError:
    pass


@pd.api.extensions.register_dataframe_accessor("teehr")
class GetMetricsAccessor:
    """Extends pandas DataFrame objects.

    Notes
    -----
    This class is a test. We could potentially have separate accessor
    classes for metrics and timeseries data, and only register them
    when their specific methods are called (get_metrics, get_timeseries).
    Seems that this would require less validation in the accessor classes,
    but could be confusing for the user?

    Alternative is to have a single accessor class that gets registered
    when the DuckDB class is instantiated that would contain methods for
    summarizing and plotting metrics as well as timeseries. This would require
    more validation in each method to ensure the DataFrame has the
    appropriate data.
    """

    def __init__(self, pandas_obj):
        """Initialize the class."""
        self._validate(pandas_obj)
        self.metrics_df = pandas_obj

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
        if "geometry" not in self.metrics_df.columns:
            raise AttributeError("DataFrame must have a 'geometry' column.")
        lat = self.metrics_df.geometry.y
        lon = self.metrics_df.geometry.x
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
        summary_df = self.metrics_df.groupby(group_by) \
            .describe(percentiles=percentiles).unstack().unstack() \
            .reset_index().rename(
                columns={'level_0': 'metric', 'level_1': 'summary'}
            )
        final_df = summary_df.pivot(
            index="metric",
            columns="summary",
            values=self.metrics_df[group_by].values.ravel()
        )
        return final_df
