"""Provides the teehr accessor extending pandas DataFrames"""
import itertools
from math import pi
import random
import pandas as pd

from bokeh.plotting import figure, save, show
from bokeh.palettes import Turbo256

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
        if "location_id" not in obj.columns:
            raise AttributeError("Must have 'location_id'.")
        if obj.index.size == 0:
            raise AttributeError("DataFrame must have data.")

    def timeseries_unique_values(
            self,
            variable_df: pd.DataFrame,
    ) -> dict:
        """Get dictionary of all unique values of each column."""
        columns = variable_df.columns.to_list()
        Dict = {}
        for column in columns:
            Dict[column] =variable_df[column].unique().tolist()

        return Dict

    def timeseries_default_schema(self) -> dict:
        """ Get dictionary that defines plotting schema."""
        unique_variables = self._df['variable_name'].unique().tolist()
        schema = {}
        for variable in unique_variables:
            variable_df = self._df[self._df['variable_name'] == variable]
            unique_column_vals = self.timeseries_unique_values(variable_df)
            all_list = [unique_column_vals['configuration_name'],
                        unique_column_vals['location_id']]
            res = list(itertools.product(*all_list))
            schema[variable] = res

        return schema

    def timeseries_generate_plot(self,
                                 schema: dict,
                                 df: pd.DataFrame,
                                 variable: str,
        ) -> figure:
        """Generates a single timeseries plot."""
        unique_units = df['unit_name'].unique().tolist()

        numColors = len(schema[variable])
        sampled_colors = random.sample(range(0,len(Turbo256)-1),numColors)
        palette = Turbo256
        palette_count = 0

        p = figure(title="Click legend entry to toggle display of timeseries",
            y_axis_label="{} [{}]".format(variable,unique_units[0]),
            x_axis_label="Datetime",
            x_axis_type='datetime',
            sizing_mode="stretch_width",
            tools=['xwheel_zoom','reset'],
            height = 800)

        for combo in schema[variable]:
            temp = df[(df['configuration_name'] == combo[0]) & (df['location_id'] == combo[1])]
            p.line(temp.value_time,
                    temp.value,
                    legend_label="{} - {}".format(combo[0],combo[1]),
                    line_width=1,
                    color=palette[sampled_colors[palette_count]])
            palette_count += 1

        p.xaxis.major_label_orientation = pi/4
        p.xaxis.axis_label_text_font_size = '14pt'
        p.xaxis.axis_label_text_font_style = 'bold'
        p.xaxis.major_label_text_font_size = '12pt'

        p.yaxis.axis_label_text_font_size = '14pt'
        p.yaxis.axis_label_text_font_style = 'bold'
        p.yaxis.major_label_text_font_size = '12pt'

        p.title.text_font_size = '12pt'

        p.legend.location = 'top_right'
        p.legend.label_text_font_size = '14pt'
        p.legend.border_line_width = 1
        p.legend.border_line_color = 'black'
        p.legend.border_line_alpha = 1.0
        p.legend.background_fill_color = 'white'
        p.legend.background_fill_alpha = 1.0
        p.legend.click_policy = 'hide'

        show(p)

        return

    def timeseries_plot(self):
        """Calls workflow to generate plot(s) based on number of unique values in self._df['variable_name'] column."""
        schema = self.timeseries_default_schema()
        for variable in schema.keys():
            df_variable = self._df[self._df['variable_name'] == variable]
            self.timeseries_generate_plot(schema=schema, df=df_variable, variable=variable)