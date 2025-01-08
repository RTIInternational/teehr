"""Provides the teehr accessor extending pandas DataFrames."""
import itertools
from math import pi
import pandas as pd
import geopandas as gpd
import logging
from pathlib import Path

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
                raise NotImplementedError(f"""
                    Locations mapping does not currently support input of
                    type = {type(obj)}. Use to_geopandas() instead.
                """)

            # convert given crs to web mercator [EPSG:3857]
            target_crs = 'EPSG:3857'
            self._gdf.to_crs(target_crs, inplace=True)

        elif obj.attrs['table_type'] == 'location_attributes':

            # check for expected fields
            fields_list = obj.attrs['fields']
            missing = []
            for field in fields_list:
                if field not in obj.columns:
                    missing.append(field)
            if len(missing) != 0:
                raise AttributeError(f"""
                    DataFrame with table_type == 'location_attributes' is
                    missing expected column(s): {missing}
                """)

            # check for data
            if obj.index.size == 0:
                raise AttributeError("GeoDataFrame must have data.")

            # convert to gdf if given df (not implemented)
            if not (isinstance(obj, gpd.GeoDataFrame)):
                raise NotImplementedError(f"""
                    Location attributes mapping does not currently support
                    input of type = {type(obj)}. Use to_geopandas() instead.
                """)

            # convert given crs to web mercator [EPSG:3857]
            target_crs = 'EPSG:3857'
            self._gdf.to_crs(target_crs, inplace=True)

        elif obj.attrs['table_type'] == 'location_crosswalks':

            # check for expected fields
            fields_list = obj.attrs['fields']
            missing = []
            for field in fields_list:
                if field not in obj.columns:
                    missing.append(field)
            if len(missing) != 0:
                raise AttributeError(f"""
                    DataFrame with table_type == 'location_crosswalks' is
                    missing expected column(s): {missing}
                """)

            # check for data
            if obj.index.size == 0:
                raise AttributeError("GeoDataFrame must have data.")

            # convert to gdf if given df (not implemented)
            if not (isinstance(obj, gpd.GeoDataFrame)):
                raise NotImplementedError(f"""
                    Location attributes mapping does not currently support
                    input of type = {type(obj)}. Use to_geopandas() instead.
                """)

            # convert given crs to web mercator [EPSG:3857]
            target_crs = 'EPSG:3857'
            self._gdf.to_crs(target_crs, inplace=True)

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
        plot.xaxis.axis_label_text_font_style = 'bold'

        # y-axis
        plot.yaxis.axis_label_text_font_style = 'bold'

        # title
        plot.title.text_font_size = '12pt'

        # legend
        plot.legend.location = 'top_right'
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
            height=800
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
        This method calls ``_timeseries_schema`` to get the plotting
        schema and ``_timeseries_generate_plot`` to generate each plot. It
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
        logger.info("Assembling geodata for locations mapping...")
        geo_data = {}
        geo_data['id'] = self._gdf['id'].tolist()
        geo_data['name'] = self._gdf['name'].tolist()
        geo_data['x'] = self._gdf.geometry.x.values.tolist()
        geo_data['y'] = self._gdf.geometry.y.values.tolist()
        logger.info("Locations geodata assembled.")

        return geo_data

    def _location_get_bounds(
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
            ("id", "@id"),
            ("name", "@name")
            ]

        # get axes bounds
        axes_bounds = self._location_get_bounds(geo_data=geo_data)

        # generate basemap
        p = figure(
            x_range=axes_bounds['x_space'],
            y_range=axes_bounds['y_space'],
            x_axis_type="mercator",
            y_axis_type="mercator",
            tooltips=tooltips,
            tools="pan, wheel_zoom, reset"
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

    def locations_map(self, output_dir=None):
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

    def _location_attributes_format_points(self) -> dict:
        """Format location_attributes data for use in mapping method."""
        logger.info("Assembling geodata for location attributes mapping...")
        geo_data = {}
        locations = self._gdf['location_id'].unique()
        for location in locations:
            local_attributes = {}
            location_gdf = self._gdf[self._gdf['location_id'] == location]
            attributes = location_gdf['attribute_name'].unique()
            for attribute in attributes:
                row = location_gdf[location_gdf['attribute_name'] == attribute]
                local_attributes[attribute] = row['value'].values[0]
                if attribute == attributes[-1]:
                    local_attributes['x'] = row.geometry.x.values[0]
                    local_attributes['y'] = row.geometry.y.values[0]
                    local_attributes['location_id'] = location
            geo_data[location] = local_attributes
        logger.info("Location attributes geodata assembled.")
        logger.info("Checking for duplicate attributes...")
        self._location_attributes_check_duplicates(geo_data=geo_data)

        return geo_data

    def _location_attributes_check_duplicates(
        self,
        geo_data: dict
    ):
        """Check for duplicate attributes."""
        for location in geo_data.keys():
            location_data = self._gdf[self._gdf['location_id'] == location]
            attribute_counts = location_data['attribute_name'].value_counts()
            for i in range(len(attribute_counts)):
                attribute_val = attribute_counts.iloc[i]
                attribute_name = attribute_counts.index[i]
                if attribute_val > 1:
                    logger.warning(f"""
        Location = {location} has multiple entries for
        attribute = {attribute_name}. Remove duplicates to avoid unintended
        results.
                                   """)

    def _location_attributes_get_tooltips(
            self,
            geo_data: dict
    ) -> list:
        """Dynamically create tooltips for location_attributes."""
        # extract attributes from geo_data
        all_attributes = []
        for location in geo_data.keys():
            attributes = geo_data[location]
            for attribute in attributes.keys():
                all_attributes.append(attribute)

        # remove duplicates and x/y
        unique_attributes = list(set(all_attributes))
        unique_attributes.remove('x')
        unique_attributes.remove('y')

        # format tuples for tooltips
        tooltips = []
        for attribute in unique_attributes:
            entry = (f"{attribute}", f"@{attribute}")
            tooltips.append(entry)

        return tooltips

    def _location_attributes_get_bounds(
            self,
            geo_data: dict
    ) -> dict:
        """Obtain axes bounds for location_attributes mapping."""
        x_list = []
        y_list = []
        for location in geo_data.keys():
            attributes = geo_data[location]
            x_list.append(attributes['x'])
            y_list.append(attributes['y'])
        min_x = min(x_list)
        max_x = max(x_list)
        min_y = min(y_list)
        max_y = max(y_list)
        x_buffer = abs((max_x - min_x)*0.1)
        y_buffer = abs((max_y - min_y)*0.1)
        axes_bounds = {}
        axes_bounds['x_space'] = ((min_x - x_buffer), (max_x + x_buffer))
        axes_bounds['y_space'] = ((min_y - y_buffer), (max_y + y_buffer))

        return axes_bounds

    def _location_attributes_make_iterable(self, geo_data) -> dict:
        """Make values iterable for plotting."""
        for key, value in geo_data.items():
            if not isinstance(value, (list, tuple, set)):
                geo_data[key] = [value]

        return geo_data

    def _location_attributes_generate_map(
            self,
            geo_data: dict,
            output_dir=None
    ) -> figure:
        """Generate map for location_attributes table."""
        logger.info("Generating location attributes map...")

        # set tooltips
        tooltips = self._location_attributes_get_tooltips(geo_data=geo_data)

        # get axes bounds
        axes_bounds = self._location_attributes_get_bounds(geo_data=geo_data)

        # generate basemap
        p = figure(
            x_range=axes_bounds['x_space'],
            y_range=axes_bounds['y_space'],
            x_axis_type="mercator",
            y_axis_type="mercator",
            tooltips=tooltips,
            tools="pan, wheel_zoom, reset"
            )
        p.add_tile(xyz.OpenStreetMap.Mapnik)

        # add data per location
        for location in geo_data.keys():
            location_dict = self._location_attributes_make_iterable(
                geo_data[location]
                )
            source = ColumnDataSource(location_dict)
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
            fname = Path(output_dir, 'location_attributes_map.html')
            output_file(filename=fname, title='Location Attributes Map')
            logger.info(f"Saving location map at {output_dir}")
            save(p)
        else:
            logger.info("No output directory specified, displaying plot.")
            show(p)

        return

    def location_attributes_map(self, output_dir=None):
        """Generate location_attributes table map.

        Generate a map of location attributes and save it to the specified
        directory.

        This function checks the table type to ensure it is
        'location_attributes'. If an output directory is specified, it checks
        if the directory exists and creates it if it does not. It then formats
        the point data and generates the map.

        Parameters
        ----------
        output_dir : Path or None, optional
            The directory where the generated map will be saved. If None, the
            map will not be saved to a file. Default is None.

        Raises
        ------
        AttributeError
            If the table type is not 'location_attributes'.

        Notes
        -----
        This function relies on the following methods:

        - ``_location_attributes_format_points``: Formats the point data.
        - ``_location_attributes_generate_map``: Generates the map using the
                                                 formatted data.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.location_attributes_map(output_dir=Path('/path/to/save'))
        """
        # check table type
        if self._gdf.attrs['table_type'] != 'location_attributes':
            table_type_str = self.attrs['table_type']
            raise AttributeError(f"""
                Expected table_type == "location_attributes",
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

        # format point data
        geo_data = self._location_attributes_format_points()

        # generate map
        self._location_attributes_generate_map(geo_data=geo_data)

    def _location_crosswalks_format_points(self) -> dict:
        """Generate dictionary for point plotting."""
        logger.info("Assembling geodata for location_crosswalks mapping...")
        geo_data = {}
        geo_data['primary_id'] = self._gdf['primary_location_id'].tolist()
        geo_data['secondary_id'] = self._gdf['secondary_location_id'].tolist()
        geo_data['x'] = self._gdf.geometry.x.values.tolist()
        geo_data['y'] = self._gdf.geometry.y.values.tolist()
        logger.info("location_crosswalks geodata assembled.")

        return geo_data

    def _location_crosswalks_generate_map(
            self,
            geo_data: dict,
            output_dir=None
    ) -> figure:
        """Generate map for location_crosswalks table."""
        logger.info("Generating location_crosswalks map...")

        # set tooltips
        tooltips = [
            ("primary_location_id", "@primary_id"),
            ("secondary_location_id", "@secondary_id")
            ]

        # get axes bounds
        axes_bounds = self._location_get_bounds(geo_data=geo_data)

        # generate basemap
        p = figure(
            x_range=axes_bounds['x_space'],
            y_range=axes_bounds['y_space'],
            x_axis_type="mercator",
            y_axis_type="mercator",
            tooltips=tooltips,
            tools="pan, wheel_zoom, reset"
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
            fname = Path(output_dir, 'location_crosswalks_map.html')
            output_file(filename=fname, title='Location Crosswalks Map')
            logger.info(f"Saving location map at {output_dir}")
            save(p)
        else:
            logger.info("No output directory specified, displaying plot.")
            show(p)

        return

    def location_crosswalks_map(self, output_dir=None):
        """Generate location_crosswalks table map.

        Generate a map of location crosswalks and save it to the specified
        directory.

        This method checks the table type to ensure it is
        'location_crosswalks'. If an output directory is specified, it checks
        if the directory exists and creates it if it does not. It then
        assembles the point data and generates the map.

        Parameters
        ----------
        output_dir : Path or None, optional
            The directory where the generated map will be saved. If None, the
            map will not be saved to a file. Default is None.

        Raises
        ------
        AttributeError
            If the table type is not 'location_crosswalks'.

        Notes
        -----
        This method relies on the following methods:

        - ``_location_crosswalks_format_points``: Assembles the point data.
        - ``_location_crosswalks_generate_map``: Generates the map using the
                                               assembled data.

        Examples
        --------
        >>> obj = YourClass()
        >>> obj.location_crosswalks_map(output_dir=Path('/path/to/save'))
        """
        # check table type
        if self._gdf.attrs['table_type'] != 'location_crosswalks':
            table_type_str = self.attrs['table_type']
            raise AttributeError(f"""
                Expected table_type == "location_crosswalks",
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

        # assemble data
        geo_data = self._location_crosswalks_format_points()

        # generate map
        self._location_crosswalks_generate_map(geo_data=geo_data)
