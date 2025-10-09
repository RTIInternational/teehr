"""A script for initializing the warehouse directory with necessary table
schemas and loading data into the tables from an existing e4 Evaluation.

Notes
-----
This is essentially the same as the convert_to_iceberg utility, but hardcoded
to write to remote.
"""
from pathlib import Path

from teehr import Evaluation


def main(
    e4_evaluation_path: str | Path = None,
    remote_catalog_name: str = "iceberg",
    remote_namespace_name: str = "e0_2_location_example"  # aka schema
):
    """Run the code to initialize and load data into the warehouse schema."""
    # All other defaults should be good
    ev = Evaluation(
        local_warehouse_dir=e4_evaluation_path,
        remote_catalog_name=remote_catalog_name,
        remote_namespace_name=remote_namespace_name,
        create_local_dir=False,
        check_evaluation_version=False,
    )

    ev.set_active_catalog("remote")

    # This should create the tables in remote if they don't exist
    ev.apply_schema_migration()

    # Now copy in the e4 data.
    options = {
        "header": "true",
        "ignoreMissingFiles": "true",
        "recursiveFileLookup": "true"
    }
    dataset_dir = e4_evaluation_path / "dataset"

    units_table = ev.units
    schema = units_table.schema_func().to_structtype()
    units_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "units").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=units_sdf,
        table_name="units",
        namespace_name=remote_namespace_name
    )

    configuration_table = ev.configurations
    schema = configuration_table.schema_func().to_structtype()
    configuration_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "configurations").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=configuration_sdf,
        table_name="configurations",
        namespace_name=remote_namespace_name
    )

    variables_table = ev.variables
    schema = variables_table.schema_func().to_structtype()
    variables_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "variables").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=variables_sdf,
        table_name="variables",
        namespace_name=remote_namespace_name
    )

    attributes_table = ev.attributes
    schema = attributes_table.schema_func().to_structtype()
    attributes_sdf = ev.spark.read.format("csv").options(**options).load((dataset_dir / "attributes").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=attributes_sdf,
        table_name="attributes",
        namespace_name=remote_namespace_name
    )

    locations_table = ev.locations
    schema = locations_table.schema_func().to_structtype()
    locations_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "locations").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=locations_sdf,
        table_name="locations",
        namespace_name=remote_namespace_name
    )

    location_attrs_table = ev.location_attributes
    schema = location_attrs_table.schema_func().to_structtype()
    location_attrs_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "location_attributes").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=location_attrs_sdf,
        table_name="location_attributes",
        namespace_name=remote_namespace_name
    )

    primary_timeseries_table = ev.primary_timeseries
    schema = primary_timeseries_table.schema_func().to_structtype()
    primary_timeseries_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "primary_timeseries").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=primary_timeseries_sdf,
        table_name="primary_timeseries",
        namespace_name=remote_namespace_name
    )

    secondary_timeseries_table = ev.secondary_timeseries
    schema = secondary_timeseries_table.schema_func().to_structtype()
    secondary_timeseries_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "secondary_timeseries").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=secondary_timeseries_sdf,
        table_name="secondary_timeseries",
        namespace_name=remote_namespace_name
    )

    joined_timeseries_table = ev.joined_timeseries
    schema = joined_timeseries_table.schema_func().to_structtype()
    joined_timeseries_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "joined_timeseries").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=joined_timeseries_sdf,
        table_name="joined_timeseries",
        namespace_name=remote_namespace_name
    )

    location_crosswalk_table = ev.location_crosswalks
    schema = location_crosswalk_table.schema_func().to_structtype()
    location_crosswalk_sdf = ev.spark.read.format("parquet").options(**options).load((dataset_dir / "location_crosswalks").as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=location_crosswalk_sdf,
        table_name="location_crosswalks",
        namespace_name=remote_namespace_name
    )

    pass


if __name__ == "__main__":
    e4_evaluation_path = Path("/mnt/c/data/ciroh/teehr/e4_evaluations/e0_2_location_example")
    main(
        e4_evaluation_path=e4_evaluation_path,
        remote_catalog_name="iceberg",
        remote_namespace_name="e0_2_location_example"
    )