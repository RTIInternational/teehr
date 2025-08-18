"""A component class for generating synthetic time series."""
import logging
from typing import List

import pyspark.sql as ps
from pydantic import BaseModel as PydanticBaseModel, ConfigDict

from teehr.models.generate.base import (
    SignatureTimeseriesBaseModel,
    BenchmarkForecastBaseModel,
    TimeseriesFilter
)
from teehr.models.pydantic_table_models import Variable


logger = logging.getLogger(__name__)


class GeneratedTimeSeriesBasemodel:
    """Base model for generated time series."""

    def write(
        self,
        destination_table: str,
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
            tbl = self.ev.secondary_timeseries
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
        df.attrs['table_type'] = self.tsm.timeseries_type.__str__()
        df.attrs['fields'] = self.df.columns
        return df


# Q. Do we need a more generic base table for generated timeseries
#    (other than base_table.py)?
class SignatureTimeseries(GeneratedTimeSeriesBasemodel):
    """Generate a synthetic time series from a single timeseries."""

    def __init__(
        self,
        generator,
        method: SignatureTimeseriesBaseModel,
        input_dataframe: ps.DataFrame = None,
        update_variable_table: bool = True,
    ):
        """Generate a new timeseries according to the method class.

        Parameters
        ----------
        generator : Generator
            The generator instance.
        method : SignatureTimeseriesBaseModel
            A model defining the signature timeseries generation method.
        input_dataframe : ps.DataFrame
            The input spark DataFrame.
        update_variable_table : bool
            Whether to update the variable table.
        """
        self.df = None
        self.ev = generator.ev

        self.df = method.generate(input_timeseries_sdf=input_dataframe)

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


class BenchmarkForecast(GeneratedTimeSeriesBasemodel):
    """Generate a synthetic time series from multiple timeseries."""

    def __init__(
        self,
        generator,
        method: BenchmarkForecastBaseModel,
        reference_dataframe: ps.DataFrame,
        template_dataframe: ps.DataFrame,
        partition_by: List[str],
        output_configuration_name: str
    ):
        """Initialize and generate the timeseries."""
        self.df = None
        self.ev = generator.ev
        # self.output_tsm = method.output_tsm

        self.df = method.generate(
            ev=self.ev,
            reference_sdf=reference_dataframe,
            template_sdf=template_dataframe,
            partition_by=partition_by,
            output_configuration_name=output_configuration_name
        )
        # TODO: Add output configuration and variable names to ev?
        pass


class Generator:
    """Component class for generating synthetic data."""

    def __init__(self, ev) -> None:
        """Initialize the Generator class."""
        self.ev = ev

    def signature_timeseries(
        self,
        method: SignatureTimeseriesBaseModel,
        input_timeseries: TimeseriesFilter
    ) -> SignatureTimeseries:
        """Generate synthetic summary from a single timeseries.

        Parameters
        ----------
        method : TimeseriesGeneratorBaseModel
            The method to use for generating the timeseries.
        input_timeseries : TimeseriesFilter, optional
            The input timeseries model. The defines a unique timeseries
            that will be queried from the Evaluation and used as the
            input_dataframe. Defaults to None.

        Returns
        -------
        SignatureTimeseries
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
        return SignatureTimeseries(
            self,
            method=method,
            input_dataframe=input_dataframe
        )

    def benchmark_forecast(
        self,
        method: BenchmarkForecastBaseModel,
        reference_timeseries: TimeseriesFilter,
        template_timeseries: TimeseriesFilter,
        output_configuration_name: str
    ) -> BenchmarkForecast:
        """Generate a benchmark forecast from two timeseries."""
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

        return BenchmarkForecast(
            self,
            method=method,
            reference_dataframe=reference_dataframe,
            template_dataframe=template_dataframe,
            partition_by=partition_by,
            output_configuration_name=output_configuration_name
        )

    # What else would we want to generate?
    # def table(self) -> None:
    #     """Create a new table."""
    #     raise NotImplementedError("Table generation is not implemented yet.")
