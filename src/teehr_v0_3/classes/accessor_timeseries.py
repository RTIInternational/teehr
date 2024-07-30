"""Provides the teehr timeseries accessor extending pandas DataFrames."""
import itertools

import pandas as pd
from bokeh.plotting import figure, output_file, save, show
from bokeh.palettes import Dark2_5 as palette


@pd.api.extensions.register_dataframe_accessor("teehr")
class GetTimeseriesAccessor:
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

    You need to run the following code to show plots in Jupyter notebooks:
    ```
    from bokeh.io import output_notebook
    output_notebook()
    ```
    """

    def __init__(self, pandas_obj: pd.DataFrame):
        """Initialize the class."""
        self._validate(pandas_obj)
        self.timeseries_df = pandas_obj

    @staticmethod
    def _validate(timeseries_df: pd.DataFrame):
        """Validate the DataFrame object columns."""
        if "primary_location_id" not in timeseries_df.columns:
            raise AttributeError("Must have 'primary_location_id'.")
        if timeseries_df.index.size == 0:
            raise AttributeError("DataFrame must have data.")

    @property
    def center(self):
        """Some property."""
        if "geometry" not in self.timeseries_df.columns:
            raise AttributeError("DataFrame must have a 'geometry' column.")
        lat = self.timeseries_df.geometry.y
        lon = self.timeseries_df.geometry.x
        return (float(lon.mean()), float(lat.mean()))

    def scatter_plot(self):
        """Create a scatter plot."""
        print("About to create a DataFrame-based scatter plot!")
        pass

    def plot_forecasts(
        self,
        primary_location_id: str,
        variable_name: str,
        measurement_unit: str,
        configuration: str,
        output_filepath: str = None,
    ) -> figure:
        """Create a timeseries plot of forecasts vs. observations."""
        # output_file("test_plot.html")

        plot_df = self.timeseries_df[
            (self.timeseries_df["primary_location_id"] == primary_location_id) &
            (self.timeseries_df["variable_name"] == variable_name) &
            (self.timeseries_df["measurement_unit"] == measurement_unit) &
            (self.timeseries_df["configuration"] == configuration)
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
            save(p, filename=output_filepath)
        else:
            show(p)
