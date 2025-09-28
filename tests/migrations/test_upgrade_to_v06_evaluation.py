"""Tests for Iceberg."""
from pathlib import Path
import shutil

import pytest

import teehr
from teehr.utilities.convert_to_iceberg import convert_evaluation

# Note. This test writes to the test/data directory, then cleans up after
# itself. If you do not let the test finish, you may need to manually
# delete the created directories.


def test_upgrade_evaluation():
    """Test upgrading a pre-v0.6 evaluation to v0.6."""
    v04_ev_dir = Path("tests", "data", "v0_4_e0_evaluation")

    try:
        # This should raise an error due to the version.
        with pytest.raises(ValueError):
            ev = teehr.Evaluation(
                local_warehouse_dir=v04_ev_dir,
            )

        convert_evaluation(v04_ev_dir)

        # Now we should be able to load the evaluation and read from the warehouse.
        ev = teehr.Evaluation(
            local_warehouse_dir=v04_ev_dir,
        )

        # Test a spark query.
        attribute_names = [row.attribute_name for row in ev.spark.sql(f"""
            SELECT DISTINCT(attribute_name) FROM {ev.catalog_name}.{ev.schema_name}.location_attributes
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
                SELECT l.id, l.name, ST_GeomFromWKB(l.geometry) AS geometry, la.*
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

    finally:
        # Clean up the test data directory after the test is done.
        shutil.rmtree(v04_ev_dir / "warehouse")
        shutil.rmtree(v04_ev_dir / "migrations")

        # Update the version file.
        version_file = Path(v04_ev_dir) / "version"
        with open(version_file, "w") as f:
            f.write("v0.4.13")


if __name__ == "__main__":
    test_upgrade_evaluation()
