"""Test module to exploare UDFs in PySpark using the DataFrame API."""
from teehr import Evaluation
from pathlib import Path

from pyspark.sql.functions import pandas_udf
import pandas as pd
from pyspark.sql.types import IntegerType

TEST_STUDY_DIR = Path(Path().home(), "temp", "test_study")

eval = Evaluation(dir_path=TEST_STUDY_DIR)
spark = eval.spark

joined_df = (
    spark.read.format("parquet")
    .option("recursiveFileLookup", "true")
    .option("mergeSchema", "true")
    .load(
        str(eval.joined_timeseries_dir)
    )
)


@pandas_udf(returnType=IntegerType())
def test_udf(s: pd.Series) -> int:
    """Test sumation udf."""
    return s.sum()


(
    joined_df.filter("primary_location_id = 'gage-C'")
    .groupBy("primary_location_id")
    .agg(test_udf("primary_value").alias("sum"))
    .show()
)
