"""Provides the teehr accessor extending pandas DataFrames."""
import itertools
from math import pi
import pandas as pd
import logging
from pathlib import Path

from bokeh.plotting import figure, save, output_file, show
from bokeh.palettes import colorblind

logger = logging.getLogger(__name__)


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
        """Validate the DataFrame object."""
        if 'table_type' not in obj.attrs:
            raise AttributeError(
                "No DataFrame Attribute 'table_type' defined."
                )

        if obj.attrs['table_type'] == 'timeseries':

            # check for expected fields
            fields_list = obj.attrs['fields']
            missing = []
            for field in fields_list:
                if field not in obj.columns:
                    missing.append(field)
            if len(missing) != 0:
                raise AttributeError(f"""
                    DataFrame with table_type == 'timeseries' is missing
                    expected column(s): {missing}
                """)
            # check for data
            if obj.index.size == 0:
                raise AttributeError("DataFrame must have data.")

        elif obj.attrs['table_type'] == 'joined_timeseries':

            # TO-DO: add validation

            raise NotImplementedError(
                "Joined_timeseries methods must be implemented."
            )

        elif obj.attrs['table_type'] == 'location':

            # TO-DO: add validation

            raise NotImplementedError(
                "Location methods must be implemented."
            )

        elif obj.attrs['table_type'] == 'metrics':

            # TO-DO: add validation

            raise NotImplementedError(
                "Metrics methods must be implemented."
            )

        else:
            table_type_str = obj.attrs['table_type']
            raise AttributeError(f"""
                Invalid table type:{table_type_str}. Visualization not
                supported.
            """)

    def _get_unique_values(
            self,
            variable_df: pd.DataFrame,
    ) -> dict:
        """Get dictionary of all unique values of each column."""
        logger.info("Retrieving unique values from DataFrame.")
        columns = variable_df.columns.to_list()
        unique_dict = {}
        for column in columns:
            unique_dict[column] = variable_df[column].unique().tolist()

        return unique_dict

    def _timeseries_schema(self) -> dict:
        """Get dictionary that defines plotting schema."""
        logger.info("Retrieving default plotting schema.")
        unique_variables = self._df['variable_name'].unique().tolist()
        raw_schema = {}
        filtered_schema = {}

        # get all unique combinations
        for variable in unique_variables:
            variable_df = self._df[self._df['variable_name'] == variable]
            unique_column_vals = self._get_unique_values(variable_df)
            all_list = [unique_column_vals['configuration_name'],
                        unique_column_vals['location_id']]
            res = list(itertools.product(*all_list))
            raw_schema[variable] = res

        # filter out invalid unique combinations
        for variable in unique_variables:
            valid_combos = []
            invalid_combos_count = 0
            var_df = self._df[self._df['variable_name'] == variable]
            for combo in raw_schema[variable]:
                temp = var_df[(var_df['configuration_name'] == combo[0]) &
                              (var_df['location_id'] == combo[1])]
                if not temp.empty:
                    valid_combos.append(combo)
                else:
                    invalid_combos_count += 1
            filtered_schema[variable] = valid_combos
            if invalid_combos_count > 0:
                logger.info(f"""
                    Removed {invalid_combos_count} invalid combinations from
                    the schema.
                """)

        return filtered_schema

    def _timeseries_generate_plot(self,
                                  schema: dict,
                                  df: pd.DataFrame,
                                  variable: str,
                                  output_dir: None,
                                  ) -> figure:
        """Generate a single timeseries plot."""
        logger.info("Generating timeseries plot.")

        unique_units = df['unit_name'].unique().tolist()

        palette = itertools.cycle(colorblind['Colorblind'][8])

        p = figure(title="Click legend entry to toggle display of timeseries",
                   y_axis_label=f"{variable} [{unique_units[0]}]",
                   x_axis_label="Datetime",
                   x_axis_type='datetime',
                   sizing_mode="stretch_width",
                   tools=['xwheel_zoom', 'reset'],
                   height=800)

        for combo in schema[variable]:
            logger.info(f"Processing combination: {combo}")
            temp = df[(df['configuration_name'] == combo[0]) &
                      (df['location_id'] == combo[1])]
            if not temp.empty:
                logger.info(f"Plotting data for combination: {combo}")
                p.line(temp.value_time,
                       temp.value,
                       legend_label=f"{combo[0]} - {combo[1]}",
                       line_width=1,
                       color=next(palette))
            else:
                logger.warning(f"No data for combination: {combo}")

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

        if output_dir is not None:
            fname = Path(output_dir, f'timeseries_plot_{variable}.html')
            output_file(filename=fname, title=f'Timeseries Plot [{variable}]')
            logger.info(f"Saving timeseries plot at {output_dir}")
            # print('im saving')
            save(p)
        else:
            logger.info("No output directory specified, displaying plot.")
            # print('im showing')
            show(p)

        return

    def timeseries_plot(self,
                        output_dir=None):
        """
        Generate and save TS plots for each unique variable in theDataFrame.

        This method generates timeseries plots for each unique variable in the
        DataFrame's 'variable_name' column. The plots are saved to the
        specified output directory if provided. If the output directory does
        not exist, it will be created.

        Parameters
        ----------
        output_dir : pathlib.Path or None, optional
            The directory where the plots will be saved. If None, the plots
            will be displayed interactively. Default is None.

        Returns
        -------
        None

        Notes
        -----
        This method calls `_timeseries_schema` to get the plotting
        schema and `_timeseries_generate_plot` to generate each plot. It
        ensures the output directory exists before saving the plots.
        """
        if self._df.attrs['table_type'] != 'timeseries':
            table_type_str = self.attrs['table_type']
            raise AttributeError(f"""
                Expected table_type == "timeseries",
                got table_type = {table_type_str}
            """)

        if output_dir is not None:
            if output_dir.exists():
                logger.info("Specified save directory is valid.")
            else:
                logger.info(""""
                    Specified directory does not exist.
                    Creating new directory to store figure.
                """)
                Path(output_dir).mkdir(parents=True, exist_ok=True)

        schema = self._timeseries_schema()
        for variable in schema.keys():
            df_variable = self._df[self._df['variable_name'] == variable]
            self._timeseries_generate_plot(schema=schema,
                                           df=df_variable,
                                           variable=variable,
                                           output_dir=output_dir)
