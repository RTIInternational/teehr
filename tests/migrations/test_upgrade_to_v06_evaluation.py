"""Tests for Iceberg."""
from pathlib import Path
import shutil
import tempfile

import pytest

import teehr
from teehr.utilities.convert_to_iceberg import convert_evaluation


def test_upgrade_evaluation(tmpdir):
    """Test upgrading a pre-v0.6 evaluation to v0.6."""
    # Copy test pre-v0.6 evaluation into the temp directory.
    v04_ev_dir = Path(tmpdir) / "v0_4_e0_evaluation"
    shutil.copytree(Path("tests", "data", "v0_4_e0_evaluation"), v04_ev_dir)

    # This should raise an error due to the version.
    with pytest.raises(Exception):
        ev = teehr.Evaluation(
            dir_path=v04_ev_dir,
        )

    convert_evaluation(v04_ev_dir)

    # Now we should be able to load the evaluation and read from the warehouse.
    ev = teehr.Evaluation(
        dir_path=v04_ev_dir,
    )

    # Test a spark query.
    attribute_names = [row.attribute_name for row in ev.spark.sql(f"""
        SELECT DISTINCT(attribute_name) FROM {ev.local_catalog.catalog_name}.{ev.local_catalog.namespace_name}.location_attributes
    """).collect()]
    attribute_names_sql = ", ".join([f"'{name}'" for name in attribute_names])

    ev.spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW locations_view AS (
            WITH location_attributes_pivot AS (
                SELECT *
                FROM (
                    SELECT location_id, attribute_name, value
                    FROM local.teehr.location_attributes
                ) src
                PIVOT (
                    max(value) FOR attribute_name IN ({attribute_names_sql})
                )
            )
            SELECT l.id, l.name, ST_GeomFromWKB(l.geometry) AS geometry, la.*
            FROM local.teehr.locations l
            LEFT JOIN location_attributes_pivot la
            ON l.id = la.location_id
            WHERE l.id IS NOT NULL
            AND la.location_id IS NOT NULL
        )
    """)

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
