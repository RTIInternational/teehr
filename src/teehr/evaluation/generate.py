"""A component class for generating synthetic time series."""
import logging
from typing import Union, List
from datetime import datetime, timedelta

import pyspark.sql.functions as F

from teehr.models.filters import FilterBaseModel
from teehr.models.generate.base import (
    SignatureGeneratorBaseModel,
    BenchmarkGeneratorBaseModel
)
from teehr.models.pydantic_table_models import Variable
from teehr.generate.utils import construct_signature_dataframe


logger = logging.getLogger(__name__)


class GeneratedTimeSeriesBasemodel:
    """Base model for generated time series."""

    def write(
        self,
        destination_table: str = "primary_timeseries",
        write_mode: str = "append",
        drop_duplicates: bool = True
    ):
        """Write the generated DataFrame to a specified table.

        Parameters
        ----------
        destination_table : str
            The name of the destination table to write to.
        write_mode : str
            The write mode for the DataFrame (e.g., "append", "overwrite").
        """
        if destination_table == "primary_timeseries":
            tbl = self._ev.primary_timeseries
        elif destination_table == "secondary_timeseries":
            # Note. This assumes the location_id's in self.sdf
            # are in the crosswalk table secondary column.
            tbl = self._ev.secondary_timeseries
            if "member" not in self.sdf.columns:
                self.sdf = self.sdf.withColumn("member", F.lit(None))
        else:
            raise ValueError(
                f"Invalid destination table: {destination_table}"
                " Must be one of: primary_timeseries, secondary_timeseries"
            )
        validated_df = self._ev.validate.schema(
            sdf=self.sdf,
            table_schema=tbl.schema_func(),
            drop_duplicates=drop_duplicates,
            foreign_keys=tbl.foreign_keys,
            uniqueness_fields=tbl.uniqueness_fields,
            add_missing_columns=True
        )
        self._ev.write.to_warehouse(
            source_data=validated_df,
            table_name=tbl.table_name,
            write_mode=write_mode,
            uniqueness_fields=tbl.uniqueness_fields
        )

    def to_pandas(self):
        """Return Pandas DataFrame."""
        df = self.sdf.toPandas()
        # df.attrs['table_type'] = self.tsm.timeseries_type.__str__()
        df.attrs['fields'] = self.sdf.columns
        return df

    def to_sdf(self):
        """Return PySpark DataFrame.

        The PySpark DataFrame can be further processed using PySpark. Note,
        PySpark DataFrames are lazy and will not be executed until an action
        is called.  For example, calling `show()`, `collect()` or toPandas().
        This can be useful for further processing or analysis.
        """
        return self.sdf


class GeneratedTimeseries(GeneratedTimeSeriesBasemodel):
    """Component class for generating synthetic data."""

    def __init__(self, ev) -> None:
        """Initialize the Generator class."""
        self._ev = ev
        self.sdf = None

    def signature_timeseries(
        self,
        method: SignatureGeneratorBaseModel,
        input_table_name: str,
        start_datetime: Union[str, datetime],
        end_datetime: Union[str, datetime],
        input_table_filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
        timestep: Union[str, timedelta] = "1 hour",
        update_variable_table: bool = True,
        fillna: bool = False,
        dropna: bool = True
    ):
        """Generate synthetic summary from a single timeseries.

        Parameters
        ----------
        method : TimeseriesGeneratorBaseModel
            The method to use for generating the timeseries.
        input_table_name : str
            The name of the input table to query the timeseries from.
        start_datetime : Union[str, datetime]
            The start datetime for the generated timeseries.
            If provided as a str, the format must be supported by PySpark's
            ``to_timestamp`` function, such as "yyyy-MM-dd HH:mm:ss".
        end_datetime : Union[str, datetime]
            The end datetime for the generated timeseries.
            If provided as a str, the format must be supported by PySpark's
            ``to_timestamp`` function, such as "yyyy-MM-dd HH:mm:ss".
        input_table_filters : Union[
                str, dict, FilterBaseModel, List[Union[str, dict, FilterBaseModel]]
            ], optional
            The input table filter(s) that define a timeseries
            that will be used as the input_dataframe.
        timestep : Union[str, timedelta], optional
            The timestep for the generated timeseries. Defaults to "1 hour".
        update_variable_table : bool, optional
            Whether to update the variable table. Defaults to True.
        fillna : bool, optional
            Whether to forward and back-fill NaN values. Defaults to False.
        dropna : bool, optional
            Whether to drop rows with NaN values. Defaults to True.

        Returns
        -------
        GeneratedTimeseries
            The generated timeseries class object.

        Notes
        -----
        This method operates on a single timeseries
        (e.g., Climatology, Normals, Detrending, etc.)

        The output variable name is derived automatically based on the input
        variable name, and added to the Evaluation if it does not exist.

        The variable naming convention follows the pattern:
        <variable>_<temporal_resolution>_<summary_statistic>

        Example
        -------
        Generate a daily climatology timeseries from the primary_timeseries.

        >>> from teehr import SignatureTimeseriesGenerators as sts

        Define the signature timeseries method

        >>> ts_normals = sts.Normals()
        >>> ts_normals.temporal_resolution = "day_of_year"
        >>> ts_normals.summary_statistic = "mean"

        Generate the signature timeseries, operating on the primary_timeseries.

        >>> ev.generate.signature_timeseries(
        >>>     method=ts_normals,
        >>>     input_table_name="primary_timeseries",
        >>>     start_datetime="1924-11-19 12:00:00",
        >>>     end_datetime="2024-11-21 13:00:00",
        >>> ).write()
        """
        # input_dataframe = self._ev.filter(table_filter=input_table_filter)
        input_dataframe = self._ev.read.from_warehouse(
            table_name=input_table_name,
            filters=input_table_filters
        ).to_sdf()
        if input_dataframe.isEmpty():
            raise ValueError(
                "Input DataFrame is empty!"
                " Check the arguments of the input_table_filter."
            )

        output_dataframe = construct_signature_dataframe(
            spark=self._ev.spark,
            input_dataframe=input_dataframe,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            timestep=timestep
        )
        self.sdf = method.generate(
            input_dataframe=input_dataframe,
            output_dataframe=output_dataframe,
            fillna=fillna,
            dropna=dropna
        )
        if update_variable_table is True:
            variable_names = self.sdf.select(
                "variable_name"
            ).distinct().collect()
            variable_names = [row.variable_name for row in variable_names]
            for output_variable_name in variable_names:
                self._ev.variables.add(
                    Variable(
                        name=output_variable_name,
                        long_name="Generated signature timeseries variable"
                    )
                )

        return self

    def benchmark_forecast(
        self,
        method: BenchmarkGeneratorBaseModel,
        reference_table_name: str,
        template_table_name: str,
        output_configuration_name: str,
        reference_table_filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
        template_table_filters: Union[
            str, dict, FilterBaseModel,
            List[Union[str, dict, FilterBaseModel]]
        ] = None,
    ):
        """Generate a benchmark forecast from two timeseries.

        Parameters
        ----------
        method : BenchmarkGeneratorBaseModel
            The method to use for generating the benchmark forecast.
        reference_table_name : str
            The name of the reference table to query the timeseries from.
        template_table_name : str
            The name of the template table to query the timeseries from.
        output_configuration_name : str
            The configuration name for the generated benchmark forecast.
        reference_table_filters : Union[
                str, dict, FilterBaseModel, List[Union[str, dict, FilterBaseModel]]
            ], optional
            The reference table filter(s) defining the timeseries
            containing the values to assign to the template timeseries.
        template_table_filters : Union[
                str, dict, FilterBaseModel, List[Union[str, dict, FilterBaseModel]]
            ], optional
            The template table filter(s) that defines the timeseries
            containing the forecast structure
            (lead time, time interval, issuance frequency, etc) to use for
            the benchmark.

        Returns
        -------
        GeneratedTimeseries
            The generated timeseries class object.

        Example
        -------
        Generate a Climatology benchmark forecast using a previously generated
        climatology timeseries as the reference and the secondary_timeseries
        as the template forecast.

        >>> from teehr import BenchmarkForecastGenerators as bmf

        Define the benchmark forecast method

        >>> ref_fcst = bmf.ReferenceForecast()
        >>> ref_fcst.aggregate_reference_timeseries = True

        Specify the tables and optional filters that define the reference
        and template timeseries.

        >>> reference_table_name = "primary_timeseries"
        >>> reference_filters = [
        >>>     "variable_name = 'streamflow_hour_of_year_mean'",
        >>>     "unit_name = 'ft^3/s'"
        >>> ]

        >>> template_table_name = "secondary_timeseries"
        >>> template_filters = [
        >>>     "variable_name = 'streamflow_hourly_inst'",
        >>>     "unit_name = 'ft^3/s'",
        >>>     "member = '1993'"
        >>> ]

        Generate the benchmark forecast timeseries and write to secondary_timeseries,
        with the configuration name 'benchmark_forecast_hourly_normals'.

        >>> ev.generate.benchmark_forecast(
        >>>     method=ref_fcst,
        >>>     reference_table_name=reference_table_name,
        >>>     reference_table_filters=reference_filters,
        >>>     template_table_name=template_table_name,
        >>>     template_table_filters=template_filters,
        >>>     output_configuration_name="benchmark_forecast_hourly_normals"
        >>> ).write(destination_table="secondary_timeseries")
        """
        reference_dataframe = self._ev.read.from_warehouse(
            table_name=reference_table_name,
            filters=reference_table_filters
        ).to_sdf()
        template_dataframe = self._ev.read.from_warehouse(
            table_name=template_table_name,
            filters=template_table_filters
        ).to_sdf()

        if reference_table_name == "primary_timeseries":
            partition_by = self._ev.primary_timeseries.uniqueness_fields
        elif reference_table_name == "secondary_timeseries":
            partition_by = self._ev.secondary_timeseries.uniqueness_fields
        partition_by.remove("value_time")

        if reference_dataframe.isEmpty():
            raise ValueError(
                "Reference DataFrame is empty!"
                " Check the parameters of the reference_timeseries."
            )
        if template_dataframe.isEmpty():
            raise ValueError(
                "Template DataFrame is empty!"
                " Check the parameters of the template_timeseries."
            )

        self.sdf = method.generate(
            ev=self._ev,
            reference_sdf=reference_dataframe,
            template_sdf=template_dataframe,
            partition_by=partition_by,
            output_configuration_name=output_configuration_name
        )
        return self

    # What else would we want to generate?
    # def table(self) -> None:
    #     """Create a new table."""
    #     raise NotImplementedError("Table generation is not implemented yet.")
