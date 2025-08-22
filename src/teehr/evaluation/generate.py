"""A component class for generating synthetic time series."""
import logging
from typing import Union
from datetime import datetime, timedelta

import pyspark.sql.functions as F

from teehr.models.generate.base import (
    SignatureGeneratorBaseModel,
    BenchmarkGeneratorBaseModel,
    TimeseriesFilter
)
from teehr.models.pydantic_table_models import Variable
from teehr.generate.utils import construct_signature_dataframe


logger = logging.getLogger(__name__)


class GeneratedTimeSeriesBasemodel:
    """Base model for generated time series."""

    def write(
        self,
        destination_table: str = "primary_timeseries",
        write_mode="append"
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
            tbl = self.ev.primary_timeseries
        elif destination_table == "secondary_timeseries":
            # Note. This assumes the location_id's in self.df
            # are in the crosswalk table secondary column.
            tbl = self.ev.secondary_timeseries
            if "member" not in self.df.columns:
                self.df = self.df.withColumn("member", F.lit(None))
        else:
            raise ValueError(
                f"Invalid destination table: {destination_table}"
                " Must be one of: primary_timeseries, secondary_timeseries"
            )
        validated_df = tbl._validate(df=self.df)
        tbl._write_spark_df(validated_df, write_mode=write_mode)

    def to_pandas(self):
        """Return Pandas DataFrame."""
        df = self.df.toPandas()
        # df.attrs['table_type'] = self.tsm.timeseries_type.__str__()
        df.attrs['fields'] = self.df.columns
        return df

    def to_sdf(self):
        """Return PySpark DataFrame.

        The PySpark DataFrame can be further processed using PySpark. Note,
        PySpark DataFrames are lazy and will not be executed until an action
        is called.  For example, calling `show()`, `collect()` or toPandas().
        This can be useful for further processing or analysis.
        """
        return self.df


class GeneratedTimeseries(GeneratedTimeSeriesBasemodel):
    """Component class for generating synthetic data."""

    def __init__(self, ev) -> None:
        """Initialize the Generator class."""
        self.ev = ev
        self.df = None

    def signature_timeseries(
        self,
        method: SignatureGeneratorBaseModel,
        input_timeseries: TimeseriesFilter,
        start_datetime: Union[str, datetime],
        end_datetime: Union[str, datetime],
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
        input_timeseries : TimeseriesFilter, optional
            The input timeseries model. The defines a unique timeseries
            that will be queried from the Evaluation and used as the
            input_dataframe. Defaults to None.
        start_datetime : Union[str, datetime]
            The start datetime for the generated timeseries.
        end_datetime : Union[str, datetime]
            The end datetime for the generated timeseries.
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
        """
        input_dataframe = self.ev.sql(
            query=input_timeseries.to_query(),
            create_temp_views=[
                f"{input_timeseries.table_name}"
            ]
        )
        if input_dataframe.isEmpty():
            raise ValueError(
                "Input DataFrame is empty!"
                " Check the parameters of the input_timeseries."
            )

        output_dataframe = construct_signature_dataframe(
            spark=self.ev.spark,
            input_dataframe=input_dataframe,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            timestep=timestep
        )
        self.df = method.generate(
            input_dataframe=input_dataframe,
            output_dataframe=output_dataframe,
            fillna=fillna,
            dropna=dropna
        )
        if update_variable_table is True:
            variable_names = self.df.select(
                "variable_name"
            ).distinct().collect()
            variable_names = [row.variable_name for row in variable_names]
            for output_variable_name in variable_names:
                self.ev.variables.add(
                    Variable(
                        name=output_variable_name,
                        long_name="Generated signature timeseries variable"
                    )
                )

        return self

    def benchmark_forecast(
        self,
        method: BenchmarkGeneratorBaseModel,
        reference_timeseries: TimeseriesFilter,
        template_timeseries: TimeseriesFilter,
        output_configuration_name: str
    ):
        """Generate a benchmark forecast from two timeseries.

        Parameters
        ----------
        method : BenchmarkGeneratorBaseModel
            The method to use for generating the benchmark forecast.
        reference_timeseries : TimeseriesFilter
            The reference timeseries containing the values to use
            for the benchmark.
        template_timeseries : TimeseriesFilter
            The template timeseries containing the forecast structure
            (lead time, time interval, issuance frequency, etc) to use for
            the benchmark.
        output_configuration_name : str
            The configuration name for the generated benchmark forecast.

        Returns
        -------
        GeneratedTimeseries
            The generated timeseries class object.
        """
        reference_dataframe = self.ev.sql(
            query=reference_timeseries.to_query(),
            create_temp_views=[
                f"{reference_timeseries.table_name}"
            ]
        )
        template_dataframe = self.ev.sql(
            query=template_timeseries.to_query(),
            create_temp_views=[
                f"{template_timeseries.table_name}"
            ]
        )
        if reference_timeseries.table_name == "primary_timeseries":
            partition_by = self.ev.primary_timeseries.unique_column_set
        elif reference_timeseries.table_name == "secondary_timeseries":
            partition_by = self.ev.secondary_timeseries.unique_column_set
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

        self.df = method.generate(
            ev=self.ev,
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
