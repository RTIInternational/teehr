"""Test the Writer class."""
import tempfile

from pyspark.sql.types import StructType, StructField, StringType

from teehr import Evaluation


def test_table_writes(tmpdir):
    """Test creating a new study."""
    ev = Evaluation(local_warehouse_dir=tmpdir, create_local_dir=True)
    ev.clone_template()

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

    ev.write.to_warehouse(
        source_data=sdf,
        target_table="units",
        write_mode="append",
        # uniqueness_fields=["name"]
    )

    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_table_writes(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
