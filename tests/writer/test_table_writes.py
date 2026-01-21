"""Test the Writer class."""

from pyspark.sql.types import StructType, StructField, StringType
import pytest


@pytest.mark.read_write_evaluation_template
def test_table_writes(read_write_evaluation_template):
    """Test creating a new study."""
    ev = read_write_evaluation_template

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("long_name", StringType(), True)
    ])

    sdf = ev.spark.createDataFrame(
      data=[
        ("ft/s", "Feet per second"),
      ],
      schema=schema
    )

    df = sdf.toPandas()

    # Can pass a spark dataframe, pandas dataframe, or named view (str)
    ev.write.to_warehouse(
        source_data=df,
        table_name="units",
        write_mode="append",
    )
