"""A component class for generating synthetic time series."""
import logging

import pyspark.sql as ps
from pyspark.sql import functions as F

from teehr.models.generate.base import (
    SummaryTimeseriesBaseModel,
    TimeseriesModel
)
from teehr.models.pydantic_table_models import Variable


logger = logging.getLogger(__name__)


# Q. Do we need a more generic base table for generated timeseries
#    (other than base_table.py)?
class SummaryTimeseries:
    """Generate a synthetic time series from a single timeseries."""

    def __init__(
        self,
        generator,
        method: SummaryTimeseriesBaseModel,
        input_dataframe: ps.DataFrame = None,
        add_variable_name: bool = True
    ):
        """Initialize and generate the timeseries."""
        self.df = None
        self.tsm = method.input_tsm
        self.ev = generator.ev
        if input_dataframe is None:
            input_dataframe = generator.ev.sql(
                query=method.input_tsm.to_query(),
                create_temp_views=[
                    f"{method.input_tsm.timeseries_type}_timeseries"
                ]
            )
        # TODO: Ensure input_dataframe is not empty here.
        # Generate the new timeseries.
        # TODO: Still need to figure out where to add new domain variables.
        if add_variable_name is True:
            output_variable_name = method._get_output_variable_name(
                input_dataframe
            )
            generator.ev.variables.add(
                Variable(
                    name=output_variable_name,
                    long_name=f"Generated {output_variable_name}",
                    unit_name=method.input_tsm.unit_name,
                )
            )

        self.df = method.generate(input_timeseries_sdf=input_dataframe)

    def write(self, write_mode="append") -> None:
        """Write the generated DataFrame to a specified path."""
        if self.tsm.timeseries_type == "primary":
            tbl = self.ev.primary_timeseries
        elif self.tsm.timeseries_type == "secondary":
            tbl = self.ev.secondary_timeseries
        validated_df = tbl._validate(df=self.df)
        tbl._write_spark_df(validated_df, write_mode=write_mode)

    def to_pandas(self):
        """Return Pandas DataFrame."""
        df = self.df.toPandas()
        df.attrs['table_type'] = self.tsm.timeseries_type.__str__()
        df.attrs['fields'] = self.df.columns
        return df


class BenchmarkForecast:
    """Generate a synthetic time series from multiple timeseries."""

    def __init__(
        self,
        generator,
        method: SummaryTimeseriesBaseModel,
        reference_dataframe: ps.DataFrame = None,
        template_dataframe: ps.DataFrame = None,
    ):
        """Initialize and generate the timeseries."""
        self.df = None
        self.ev = generator.ev
        self.output_tsm = method.output_tsm
        if reference_dataframe is None:
            reference_dataframe = generator.ev.sql(
                query=method.reference_tsm.to_query(),
                create_temp_views=[
                    f"{method.reference_tsm.timeseries_type}_timeseries"
                ]
            )
        if template_dataframe is None:
            template_dataframe = generator.ev.sql(
                query=method.template_tsm.to_query(),
                create_temp_views=[
                    f"{method.template_tsm.timeseries_type}_timeseries"
                ]
            )
        if method.reference_tsm.timeseries_type == "primary":
            partition_by = self.ev.primary_timeseries.unique_column_set
        elif method.reference_tsm.timeseries_type == "secondary":
            partition_by = self.ev.secondary_timeseries.unique_column_set
        partition_by.remove("value_time")
        # TODO: Ensure dataframes are not empty here.
        # Generate the new benchmark forecast.
        self.df = method.generate(
            ev=self.ev,
            reference_sdf=reference_dataframe,
            template_sdf=template_dataframe,
            partition_by=partition_by
        )
        # TODO: Add output configuration and variable names to ev?
        pass

    def write(self, write_mode="append") -> None:
        """Write the generated DataFrame to a specified path."""
        if self.output_tsm.timeseries_type == "primary":
            tbl = self.ev.primary_timeseries
        elif self.output_tsm.timeseries_type == "secondary":
            tbl = self.ev.secondary_timeseries
        validated_df = tbl._validate(df=self.df)
        tbl._write_spark_df(validated_df, write_mode=write_mode)

    def to_pandas(self):
        """Return Pandas DataFrame."""
        df = self.df.toPandas()
        df.attrs['table_type'] = self.tsm.timeseries_type.__str__()
        df.attrs['fields'] = self.df.columns
        return df


class Generator:
    """Component class for generating synthetic data."""

    def __init__(self, ev) -> None:
        """Initialize the Generator class."""
        self.ev = ev

    def summary_timeseries(
        self,
        method: SummaryTimeseriesBaseModel,
        input_dataframe: ps.DataFrame = None
    ) -> SummaryTimeseries:
        """Generate synthetic summary from a single timeseries.

        Parameters
        ----------
        method : TimeseriesGeneratorBaseModel
            The method to use for generating the timeseries.
        input_dataframe : ps.DataFrame, optional
            The input Spark DataFrame. Defaults to None.
        input_tsm : TimeseriesModel, optional
            The input timeseries model. The defines a unique timeseries
            that will be queried from the Evaluation and used as the
            input_dataframe. Defaults to None.

        Returns
        -------
        SummaryTimeseries
            The generated timeseries object.

        Notes
        -----
        This method operates on a single timeseries
        (e.g., Climatology, Normals, Detrending, etc.)

        The output variable name is derived automatically based on the input
        variable name, and added to the Evaluation if it does not exist.

        The naming convention follows the pattern:
        <variable>_<temporal_resolution>_<summary_statistic>
        """
        return SummaryTimeseries(
            self,
            method=method,
            input_dataframe=input_dataframe
        )

    def benchmark_forecast(
        self,
        method: SummaryTimeseriesBaseModel,
        reference_dataframe: ps.DataFrame = None,
        template_dataframe: ps.DataFrame = None
    ) -> BenchmarkForecast:
        """Generate a benchmark forecast from one or more timeseries."""
        return BenchmarkForecast(
            self,
            method=method,
            reference_dataframe=reference_dataframe,
            template_dataframe=template_dataframe
        )

    # def table(self) -> None:
    #     """Create a new table."""
    #     raise NotImplementedError("Table generation is not implemented yet.")

    # What else would we want to generate?
