"""A component class for generating synthetic time series."""
import logging

import pyspark.sql as ps
from pyspark.sql import functions as F

from teehr.models.generate.base import (
    TimeseriesGeneratorBaseModel,
    TimeseriesModel
)
from teehr.models.pydantic_table_models import Variable


logger = logging.getLogger(__name__)


# Q. Do we need a more generic base table for generated timeseries
#    (other than base_table.py)?
class GeneratedTimeseries:
    """Component class for generating synthetic time series."""

    def __init__(
        self,
        generate,
        method: TimeseriesGeneratorBaseModel,
        input_dataframe: ps.DataFrame = None,
        input_tsm: TimeseriesModel = None
    ):
        """Initialize and generate the timeseries."""
        self.df = None
        self.ev = generate.ev
        self.tsm = input_tsm
        if input_dataframe is None:
            input_dataframe = generate.ev.sql(
                query=input_tsm.to_query(),
                create_temp_views=[
                    f"{input_tsm.timeseries_type}_timeseries"
                ]
            )
        # Handle the output variable name.
        input_variable_name = input_dataframe.select(
            F.first("variable_name")
        ).collect()[0][0]
        variable = input_variable_name.split("_")[0]
        output_variable_name = (
                f"{variable}_{method.temporal_resolution.value}_"
                f"{method.summary_statistic.value}"
            )
        generate.ev.variables.add(
            variable=[
                Variable(
                    name=output_variable_name,
                    long_name="Generated variable"
                ),
            ]
        )
        # Generate the new timeseries.
        self.df = method.generate(
            input_timeseries_sdf=input_dataframe,
            output_variable_name=output_variable_name
        )

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


class Generate:
    """Component class for generating synthetic data."""

    def __init__(self, ev) -> None:
        """Initialize the Generate class."""
        self.ev = ev

    def timeseries(
        self,
        method: TimeseriesGeneratorBaseModel,
        input_dataframe: ps.DataFrame = None,
        input_tsm: TimeseriesModel = None
    ) -> GeneratedTimeseries:
        """Generate synthetic timeseries data.

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
        GeneratedTimeseries
            The generated timeseries object.

        Notes
        -----
        The output variable name is derived automatically based on the input
        variable name, and added to the Evaluation if it does not exist.

        The naming convention follows the pattern:
        <variable>_<temporal_resolution>_<summary_statistic>
        """
        return GeneratedTimeseries(
            self,
            method=method,
            input_dataframe=input_dataframe,
            input_tsm=input_tsm
        )

    # def table(self) -> None:
    #     """Create a new table."""
    #     raise NotImplementedError("Table generation is not implemented yet.")

    # What else would we want to generate?
