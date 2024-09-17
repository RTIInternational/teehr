"""Provides the teehr accessor extending pandas DataFrames."""
from typing import List
import itertools

import pandas as pd
from bokeh.plotting import figure, save, show
from bokeh.palettes import Dark2_5 as palette


@pd.api.extensions.register_dataframe_accessor("teehr")
class TEEHRDataFrameAccessor:
    """Extends pandas DataFrame objects.

    Notes
    -----
    This class contains example methods for summarizing and plotting metrics
    as well as timeseries. This requires more validation in each method to
    ensure the DataFrame has the appropriate data.

    Methods operating on metrics data should start with 'metrics_' and methods
    operating on timeseries data should start with 'timeseries_'.
    """

    def __init__(self, pandas_obj):
        """Initialize the class."""
        self._df = pandas_obj
        self._validate(pandas_obj)

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
        if "geometry" not in self._df.columns:
            raise AttributeError("DataFrame must have a 'geometry' column.")
        lat = self._df.geometry.y
        lon = self._df.geometry.x
        return (float(lon.mean()), float(lat.mean()))

    def metrics_scatter_plot(self):
        """Create a scatter plot."""
        print("About to create a DataFrame-based scatter plot!")
        pass

    def metrics_box_whisker_plot(self):
        """Create a box and whisker plot."""
        print("About to create a DataFrame-based box and whisker plot!")
        pass

    def metrics_summary(
        self,
        group_by: List[str] = ["primary_location_id"],
        percentiles: List[float] = [0.5],
    ) -> pd.DataFrame:
        """Summarize the DataFrame metrics."""
        summary_df = self._df.groupby(group_by) \
            .describe(percentiles=percentiles).unstack().unstack() \
            .reset_index().rename(
                columns={'level_0': 'metric', 'level_1': 'summary'}
            )
        final_df = summary_df.pivot(
            index="metric",
            columns="summary",
            values=self._df[group_by].values.ravel()
        )
        return final_df

    def timeseries_summary(self):
        """Provide summary info about a timeseries."""
        print("About to create a DataFrame-based timeseries summary!")
        pass

    def timeseries_forecast_plot(
        self,
        primary_location_id: str,
        variable_name: str,
        measurement_unit: str,
        configuration: str,
        output_filepath: str = None,
    ) -> figure:
        """Create a timeseries plot of forecasts vs. observations."""
        plot_df = self._df[
            (self._df["primary_location_id"] == primary_location_id) &  # noqa: E501
            (self._df["variable_name"] == variable_name) &
            (self._df["measurement_unit"] == measurement_unit) &
            (self._df["configuration"] == configuration)
        ].copy()

        p = figure(
            x_axis_type="datetime",
            background_fill_color="#fafafa",
            title=f"{primary_location_id} - {variable_name}"
        )

        gps = plot_df.groupby("reference_time")

        # Colors has a list of colors which can be used in plots
        colors = itertools.cycle(palette)

        for reference_time, df in gps:
            df.sort_values("value_time", inplace=True)
            p.line(
                df["value_time"],
                df["secondary_value"],
                legend_label=str(reference_time),
                line_width=2,
                alpha=0.8,
                color=next(colors)
            )
            p.scatter(
                df["value_time"],
                df["secondary_value"],
                fill_color="white",
                size=8
            )
            p.yaxis.axis_label = measurement_unit

        obs_df = plot_df.drop_duplicates(subset=["value_time"]).copy()
        obs_df.sort_values("value_time", inplace=True)
        p.line(
            obs_df["value_time"],
            obs_df["primary_value"],
            legend_label="primary_value",
            line_width=2,
            alpha=0.5,
            color="black"
        )

        if output_filepath:
            save(p, title="Timeseries Plot", filename=output_filepath)
        else:
            show(p)
