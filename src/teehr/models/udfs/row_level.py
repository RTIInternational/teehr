"""Classes representing UDFs."""
from typing import List, Union
from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, ConfigDict
import pandas as pd
import pyspark.sql.types as T
from pyspark.sql.functions import pandas_udf
import pyspark.sql as ps
from teehr.models.udfs.udf_base import UDF_ABC, UDFBasemodel


class Month(UDF_ABC, UDFBasemodel):
    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="month"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            return col.dt.month

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class Year(UDF_ABC, UDFBasemodel):
    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="year"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            return col.dt.year

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class WaterYear(UDF_ABC, UDFBasemodel):
    input_field_name: str = Field(
        default="value_time"
    )
    output_field_name: str = Field(
        default="water_year"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:
        @pandas_udf(returnType=T.IntegerType())
        def func(col: pd.Series) -> pd.Series:
            return col.dt.year + (col.dt.month >= 10).astype(int)

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.input_field_name)
        )
        return sdf


class NormalizedFlow(UDF_ABC, UDFBasemodel):
    primary_value_field_name: str = Field(
        default="primary_value"
    )
    drainage_area_field_name: str = Field(
        default="drainage_area"
    )
    output_field_name: str = Field(
        default="normalized_flow"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        @pandas_udf(returnType=T.FloatType())
        def func(value: pd.Series, area: pd.Series) -> pd.Series:
            return value.astype(float) / area.astype(float)

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.primary_value_field_name, self.drainage_area_field_name)
        )
        return sdf

class Seasons(UDF_ABC, UDFBasemodel):
    value_time_field_name: str = Field(
        default="value_time"
    )
    season_months: dict = Field(
        default={
            "winter": [12, 1, 2],
            "spring": [3, 4, 5],
            "summer": [6, 7, 8],
            "fall": [9, 10, 11]
        }
    )
    output_field_name: str = Field(
        default="season"
    )

    def apply_to(self, sdf: ps.DataFrame) -> ps.DataFrame:

        @pandas_udf(returnType=T.StringType())
        def func(value_time: pd.Series) -> pd.Series:
            return value_time.dt.month.apply(
                lambda x: next(
                    (season for season, months in self.season_months.items() if x in months),
                    None
                )
            )

        sdf = sdf.withColumn(
            self.output_field_name,
            func(self.value_time_field_name)
        )
        return sdf

class RowLevelUDF():
    """Row level UDFs.

    Row level UDFs are applied to each row in a DataFrame.
    """
    Month = Month
    Year = Year
    WaterYear = WaterYear
    NormalizedFlow = NormalizedFlow
    Seasons = Seasons