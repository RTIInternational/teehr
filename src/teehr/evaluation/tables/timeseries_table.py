import teehr.const as const
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.utils import (
    validate_input_is_xml,
    validate_input_is_csv,
    validate_input_is_netcdf,
    validate_input_is_parquet
)
from teehr.models.filters import TimeseriesFilter
# from teehr.models.table_enums import TimeseriesFields
# from teehr.models.pydantic_table_models import Timeseries
from teehr.querying.utils import join_geometry

from pathlib import Path
from typing import Union

import logging

logger = logging.getLogger(__name__)


class TimeseriesTable(BaseTable):
    """Access methods to timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.format = "parquet"
        self.save_mode = "overwrite"
        self.partition_by = ["configuration_name", "variable_name"]
        self.filter_model = TimeseriesFilter

    def to_pandas(self):
        """Return Pandas DataFrame for Primary Timeseries."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = 'timeseries'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        return join_geometry(self.df, self.ev.locations.to_sdf())

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import primary timeseries parquet data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading primary timeseries parquet data: {in_path}")

        validate_input_is_parquet(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._load_table()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import primary timeseries csv data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            csv file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to pd.read_csv().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading primary timeseries csv data: {in_path}")

        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._load_table()

    def load_netcdf(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.nc",
        field_mapping: dict = None,
        constant_field_values: dict = None,
        **kwargs
    ):
        """Import primary timeseries netcdf data.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            netcdf file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}
        **kwargs
            Additional keyword arguments are passed to xr.open_dataset().

        Includes validation and importing data to database.

        Notes
        -----

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        """
        logger.info(f"Loading primary timeseries netcdf data: {in_path}")

        validate_input_is_netcdf(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
            **kwargs
        )
        self._load_table()

    def load_fews_xml(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.xml",
        field_mapping: dict = {
            "locationId": "location_id",
            "forecastDate": "reference_time",
            "parameterId": "variable_name",
            "units": "unit_name",
            "ensembleId": "configuration_name",
            "ensembleMemberIndex": "member",
            "forecastDate": "reference_time"
        },
        constant_field_values: dict = None,
    ):
        """Import timeseries from XML data format.

        Parameters
        ----------
        in_path : Union[Path, str]
            Path to the timeseries data (file or directory) in
            xml file format.
        pattern : str, optional (default: "**/*.xml")
            The pattern to match files.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
            Default mapping:
            {
                "locationId": "location_id",
                "forecastDate": "reference_time",
                "parameterId": "variable_name",
                "units": "unit_name",
                "ensembleId": "configuration_name",
                "ensembleMemberIndex": "member",
                "forecastDate": "reference_time"
            }
        constant_field_values : dict, optional
            A dictionary mapping field names to constant values.
            Format: {field_name: value}.

        Includes validation and importing data to database.

        Notes
        -----
        This function follows the Delft-FEWS Published Interface (PI)
        XML format.

        reference: https://publicwiki.deltares.nl/display/FEWSDOC/Dynamic+data

        The ``value`` and ``value_time`` fields are parsed automatically.

        The TEEHR Timeseries table schema includes fields:

        - reference_time
        - value_time
        - configuration_name
        - unit_name
        - variable_name
        - value
        - location_id
        - member
        """
        logger.info(f"Loading primary timeseries xml data: {in_path}")

        validate_input_is_xml(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values,
        )
        self._load_table()
