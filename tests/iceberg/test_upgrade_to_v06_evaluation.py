"""Tests for Iceberg."""
import tempfile

import teehr
from teehr.utilities.convert_to_iceberg import convert_evaluation


def test_upgrade_evaluation(tmpdir):
    """Test upgrading a pre-v0.6 evaluation to v0.6."""
    ev = teehr.Evaluation(
        dir_path=tmpdir,
        create_dir=True,
    )
    ev.clone_from_s3("e0_2_location_example")

    convert_evaluation(tmpdir)

    # Test a spark query.
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

    df = ev.spark.sql("""
        SELECT *
        FROM locations_view
    """).toPandas()

    assert df.index.size == 2
    assert df.columns.size == 29
    assert "NWRFC" in df.river_forecast_center.tolist()

    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tmpdir:
        test_upgrade_evaluation(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tmpdir
            )
        )
