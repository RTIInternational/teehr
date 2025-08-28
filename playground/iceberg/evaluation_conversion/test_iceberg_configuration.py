"""Tests for Iceberg."""
from pathlib import Path
# import tempfile

from evaluation import Evaluation

EVAL_WAREHOUSE_DIR = (
    Path.cwd() / "playground" / "iceberg" / "local-eval-warehouse"
)


def test_evaluation_conversion():
    """Test creating a new study."""
    ev = Evaluation(
        dir_path=EVAL_WAREHOUSE_DIR,
        warehouse_path=EVAL_WAREHOUSE_DIR / "warehouse",
        # create_dir=True,
    )
    # 1. Clone an existing evaluation and convert to iceberg using pre-defined
    #    schemas
    # ev.clone_from_s3("e0_2_location_example")

    # Apply schema migration. This creates the warehouse tables
    # if they don't exist.
    # ev.apply_schema_migration()

    # Convert the dataset to iceberg tables.
    # ev.convert_to_iceberg()

    # Test a sedona query.
    attribute_names = [row.attribute_name for row in ev.spark.sql(f"""
        SELECT DISTINCT(attribute_name) FROM local.db.location_attributes
    """).collect()]
    attribute_names_sql = ", ".join([f"'{name}'" for name in attribute_names])

    ev.spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW locations_view AS (
            WITH location_attributes_pivot AS (
                SELECT *
                FROM (
                    SELECT location_id, attribute_name, value
                    FROM local.db.location_attributes
                ) src
                PIVOT (
                    max(value) FOR attribute_name IN ({attribute_names_sql})
                )
            )
            SELECT l.id, l.name, l.geometry AS geometry, la.*
            FROM local.db.locations l
            LEFT JOIN location_attributes_pivot la
            ON l.id = la.location_id
            WHERE l.id IS NOT NULL
            AND la.location_id IS NOT NULL
        )
    """)

    # ST_GeomFromWKB(l.geometry)  # Can't get sedona to work with pyspark 4.0!
    # Looks like it's coming in 1.8.0: https://github.com/apache/sedona/pull/1919

    df = ev.spark.sql(f"""
        SELECT *
        FROM locations_view
    """).toPandas()

    # WHERE id = 'usgs-01013500'

    ev.spark.stop()

    pass


if __name__ == "__main__":
    test_evaluation_conversion()