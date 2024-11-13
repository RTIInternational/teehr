"""Provides the teehr accessor extending pandas DataFrames."""
import itertools
from math import pi
import pandas as pd
import geopandas as gpd
import logging
from pathlib import Path

from teehr.querying.utils import df_to_gdf

from bokeh.plotting import figure, save, output_file, show, ColumnDataSource
from bokeh.palettes import colorblind
import xyzservices.providers as xyz

logger = logging.getLogger(__name__)


@pd.api.extensions.register_dataframe_accessor("teehr")
class TEEHRDataFrameAccessor:
    """Extends pandas DataFrame objects with visualization methods.

    Notes
    -----
    This class contains example methods for summarizing and plotting metrics
    as well as timeseries. This requires more validation in each method to
    ensure the DataFrame has the appropriate data.

    Methods operating on metrics data should start with 'metrics' and methods
    operating on timeseries data should start with 'timeseries'.
    """

    def __init__(self, pandas_obj):
        """Initialize the class."""
        logger.info("Initializing new dataframe_accessor object...")
        if not (isinstance(pandas_obj, gpd.GeoDataFrame)):
            logger.info("Adding DataFrame to accessor object.")
            self._df = pandas_obj
            self._gdf = None
            self._validate(self=self, obj=pandas_obj)
            logger.info("Object validation successful.")
        else:
            logger.info("Adding GeoDataFrame to accessor object. ")
            self._df = None
            self._gdf = pandas_obj
            self._validate(self=self, obj=pandas_obj)
            logger.info("Object validation successful.")

    @staticmethod
    def _validate(self, obj):
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

        elif obj.attrs['table_type'] == 'locations':

            # check for expected fields
            fields_list = obj.attrs['fields']
            missing = []
            for field in fields_list:
                if field not in obj.columns:
                    missing.append(field)
            if len(missing) != 0:
                raise AttributeError(f"""
                    DataFrame with table_type == 'locations' is missing
                    expected column(s): {missing}
                """)

            # check for data
            if obj.index.size == 0:
                raise AttributeError("GeoDataFrame must have data.")

            # convert to gdf if given df
            if not (isinstance(obj, gpd.GeoDataFrame)):
                logger.info("""
                    Object is DataFrame. Expected GeoDataFrame. Converting to
                    GeoDataFrame...
                            """)
                geo_obj = df_to_gdf(obj)
                # reassign pandas attributes (they dont carry over)
                for attribute in obj.attrs:
                    geo_obj.attrs[attribute] = obj.attrs[attribute]
                self._gdf = geo_obj

            # convert given crs to web mercator [EPSG:3857]
            target_crs = 'EPSG:3857'
            self._gdf.to_crs(target_crs, inplace=True)

        elif obj.attrs['table_type'] == 'location_attributes':

            # TO-DO: add validation

            raise NotImplementedError(
                "Location Attributes methods must be implemented."
            )

        elif obj.attrs['table_type'] == 'location_crosswalks':

            # TO-DO: add validation

            raise NotImplementedError(
                "Location Crosswalk methods must be implemented."
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

    def _timeseries_unique_values(
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
            unique_column_vals = self._timeseries_unique_values(variable_df)
            all_list = [
                unique_column_vals['configuration_name'],
                unique_column_vals['location_id'],
                unique_column_vals['reference_time']
                ]
            res = list(itertools.product(*all_list))
            raw_schema[variable] = res

        # filter out invalid unique combinations
        for variable in unique_variables:
            valid_combos = []
            invalid_combos_count = 0
            var_df = self._df[self._df['variable_name'] == variable]
            for combo in raw_schema[variable]:
                if pd.isnull(combo[2]):  # reference_time is null
                    temp = var_df[
                        (var_df['configuration_name'] == combo[0]) &
                        (var_df['location_id'] == combo[1])
                        ]
                    if not temp.empty:
                        valid_combos.append(combo)
                    else:
                        invalid_combos_count += 1
                else:  # reference_time is not null
                    temp = var_df[
                        (var_df['configuration_name'] == combo[0]) &
                        (var_df['location_id'] == combo[1]) &
                        (var_df['reference_time'] == combo[2])
                        ]
                    if not temp.empty:
                        valid_combos.append(combo)
                    else:
                        invalid_combos_count += 1
            filtered_schema[variable] = valid_combos
            if invalid_combos_count > 0:
                logger.info(f"""
                    Removed {invalid_combos_count} invalid combinations
                    from the schema.
                """)

        return filtered_schema

    def _timeseries_format_plot(
        self,
        plot: figure,
    ) -> figure:
        """Format timeseries plot."""
        # x-axis
        plot.xaxis.major_label_orientation = pi/4
        # plot.xaxis.axis_label_text_font_size = '14pt'
        plot.xaxis.axis_label_text_font_style = 'bold'
        # plot.xaxis.major_label_text_font_size = '12pt'

        # y-axis
        # plot.yaxis.axis_label_text_font_size = '14pt'
        plot.yaxis.axis_label_text_font_style = 'bold'
        # plot.yaxis.major_label_text_font_size = '12pt'

        # # title
        # plot.title.text_font_size = '12pt'

        # legend
        plot.legend.location = 'top_right'
        # plot.legend.label_text_font_size = '14pt'
        plot.legend.border_line_width = 1
        plot.legend.border_line_color = 'black'
        plot.legend.border_line_alpha = 1.0
        plot.legend.background_fill_color = 'white'
        plot.legend.background_fill_alpha = 1.0
        plot.legend.click_policy = 'hide'

        # tools
        plot.sizing_mode = 'stretch_width'
        plot.toolbar.autohide = True

        return plot

    def _timeseries_generate_plot(
        self,
        schema: dict,
        df: pd.DataFrame,
        variable: str,
        output_dir: None,
    ) -> figure:
        """Generate a single timeseries plot."""
        logger.info("Generating timeseries plot.")

        # generate plot
        unique_units = df['unit_name'].unique().tolist()
        palette = itertools.cycle(colorblind['Colorblind'][8])
        p = figure(
            title="Click legend entry to toggle display of timeseries",
            y_axis_label=f"{variable} [{unique_units[0]}]",
            x_axis_label="Datetime",
            x_axis_type='datetime',
            tools=['xwheel_zoom', 'reset'],
            height=500
            )

        # add data to plot
        for combo in schema[variable]:
            if pd.isnull(combo[2]):  # reference_time is null
                logger.info(f"Processing combination: {combo}")
                logger.info(f"""
                            reference_time == NaT, ignoring reference_time for
                            combo: {combo}
                            """)
                temp = df[
                    (df['configuration_name'] == combo[0]) &
                    (df['location_id'] == combo[1])
                    ]
                if not temp.empty:
                    logger.info(f"Plotting data for combination: {combo}")
                    p.line(
                        temp.value_time,
                        temp.value,
                        legend_label=f"{combo[0]} - {combo[1]}",
                        line_width=1,
                        color=next(palette)
                    )
                else:
                    logger.warning(f"No data for combination: {combo}")
            else:  # reference_time is not null
                logger.info(f"Processing combination: {combo}")
                temp = df[
                    (df['configuration_name'] == combo[0]) &
                    (df['location_id'] == combo[1]) &
                    (df['reference_time'] == combo[2])
                    ]
                if not temp.empty:
                    logger.info(f"Plotting data for combination: {combo}")
                    p.line(
                        temp.value_time,
                        temp.value,
                        legend_label=f"{combo[0]} - {combo[1]} - {combo[2]}",
                        line_width=1,
                        color=next(palette)
                    )
                else:
                    logger.warning(f"No data for combination: {combo}")

        # format plot
        p = self._timeseries_format_plot(plot=p)

        # output figure
        if output_dir is not None:
            fname = Path(output_dir, f'timeseries_plot_{variable}.html')
            output_file(filename=fname, title=f'Timeseries Plot [{variable}]')
            logger.info(f"Saving timeseries plot at {output_dir}")
            save(p)
        else:
            logger.info("No output directory specified, displaying plot.")
            show(p)

        return

    def timeseries_plot(self, output_dir=None):
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
        # check table type
        if self._df.attrs['table_type'] != 'timeseries':
            table_type_str = self.attrs['table_type']
            raise AttributeError(f"""
                Expected table_type == "timeseries",
                got table_type = {table_type_str}
            """)

        # check for output location
        if output_dir is not None:
            if output_dir.exists():
                logger.info("Specified save directory is valid.")
            else:
                logger.info(""""
                    Specified directory does not exist.
                    Creating new directory to store figure.
                """)
                Path(output_dir).mkdir(parents=True, exist_ok=True)

        # generate plots
        schema = self._timeseries_schema()
        for variable in schema.keys():
            df_variable = self._df[self._df['variable_name'] == variable]
            self._timeseries_generate_plot(
                schema=schema,
                df=df_variable,
                variable=variable,
                output_dir=output_dir
                )

    def _location_format_points(self) -> dict:
        """Generate dictionary for point plotting."""
        logger.info("Assembling geodata for mapping.")
        geo_data = {}
        geo_data['id'] = self._gdf['id'].tolist()
        geo_data['name'] = self._gdf['name'].tolist()
        geo_data['x'] = self._gdf.geometry.x.values.tolist()
        geo_data['y'] = self._gdf.geometry.y.values.tolist()

        return geo_data

    def _location_get_boundaries(
        self,
        geo_data: dict
    ) -> dict:
        """Determine axes ranges using point data."""
        logger.info("Retrieving axes ranges from geodata.")
        min_x = min(geo_data['x'])
        max_x = max(geo_data['x'])
        min_y = min(geo_data['y'])
        max_y = max(geo_data['y'])
        x_buffer = abs((max_x - min_x)*0.1)
        y_buffer = abs((max_y - min_y)*0.1)
        axes_bounds = {}
        axes_bounds['x_space'] = ((min_x - x_buffer), (max_x + x_buffer))
        axes_bounds['y_space'] = ((min_y - y_buffer), (max_y + y_buffer))

        return axes_bounds

    def _location_generate_map(
        self,
        geo_data: dict,
        output_dir: None
    ) -> figure:
        """Generate location map."""
        logger.info("Generating location map.")

        # set tooltips
        tooltips = [
            ("ID", "@id"),
            ("Name", "@name"),
            ("X-Coordinate", "@x"),
            ("Y-Coordinate", "@y")
            ]

        # get axes bounds
        axes_bounds = self._location_get_boundaries(geo_data=geo_data)

        # generate basemap
        p = figure(
            x_range=axes_bounds['x_space'],
            y_range=axes_bounds['y_space'],
            x_axis_type="mercator",
            y_axis_type="mercator",
            tooltips=tooltips,
            tools="pan, wheel_zoom, reset",
            height=500
            )
        p.add_tile(xyz.OpenStreetMap.Mapnik)

        # add data
        source = ColumnDataSource(data=geo_data)
        p.scatter(
            x='x',
            y='y',
            color='blue',
            source=source,
            size=10,
            fill_alpha=1.0
            )

        # output figure
        if output_dir is not None:
            fname = Path(output_dir, 'location_map.html')
            output_file(filename=fname, title='Location Map')
            logger.info(f"Saving location map at {output_dir}")
            save(p)
        else:
            logger.info("No output directory specified, displaying plot.")
            show(p)

        return

    def location_map(self, output_dir=None):
        """
        Generate a location map and save it to the specified directory.

        Parameters
        ----------
        output_dir : str or Path, optional
            The directory where the generated map will be saved. If not
            provided, the map will not be saved. If the directory does not
            exist, it will be created.

        Raises
        ------
        AttributeError
            If the table type is not 'locations'.

        Notes
        -----
        This function checks the table type to ensure it is 'locations'. If an
        output directory is specified, it checks if the directory exists and
        creates it if it does not. The function then formats the location
        points and generates the map, saving it to the specified directory if
        provided.
        """
        # check table type
        if self._gdf.attrs['table_type'] != 'locations':
            table_type_str = self.attrs['table_type']
            raise AttributeError(f"""
                Expected table_type == "locations",
                got table_type = {table_type_str}
            """)

        # check output location
        if output_dir is not None:
            if output_dir.exists():
                logger.info("Specified save directory is valid.")
            else:
                logger.info("""
                    Specified directory does not exist.
                    Creating new directory to store figure.
                """)
                Path(output_dir).mkdir(parents=True, exist_ok=True)

        geo_data = self._location_format_points()

        # generate map
        self._location_generate_map(geo_data=geo_data, output_dir=output_dir)
