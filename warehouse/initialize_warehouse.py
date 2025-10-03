"""A script for initializing the warehouse directory with necessary schema files."""
from pathlib import Path

from teehr.utilities.apply_migrations import evolve_catalog_schema
from teehr import Evaluation


def main():
    """Run the code to initialize the warehouse schema."""
    catalog_name = "iceberg"
    namespace = "e0_2_location_example"  # aka schema, database

    # Load data from an existing local e4 Evaluation
    e4_evaluation_path = Path("/mnt/c/data/ciroh/teehr/e4_evaluations/e0_2_location_example")
    # e4_evaluation_path = Path("/mnt/c/data/ciroh/teehr/test_stuff/teehr/warehouse")  # TEMP!

    # All other defaults should be good
    ev = Evaluation(
        local_warehouse_dir=e4_evaluation_path,
        local_catalog_name="local",
        local_namespace_name=namespace,
        remote_catalog_name=catalog_name,
        remote_namespace_name=namespace,
        create_local_dir=False,
        check_evaluation_version=False,
    )

    # NOTE: This does not go to remote, clone_template only targets local dirs.
    # If a user wants to apply migrations, run scripts, and
    # clone data from the remote warehouse, they must set up a template locally.
    # ev.clone_template(
    #     catalog_name=catalog_name,
    #     namespace=namespace
    # )

    # # Now, for all table-based functions like read/query/write we can pass in
    # # the catalog, namespace, and table name to operate on:
    # # - catalog_name
    # # - namespace_name
    # # - table_name
    # remote_sdf = ev.read.from_warehouse(
    #     catalog_name="iceberg",
    #     namespace="teehr",
    #     table="units",
    #     filters="name = 'mm/s'"
    # ).to_sdf()

    # local_sdf = ev.read.from_warehouse(
    #     catalog_name="local",
    #     namespace="db",
    #     table="units"
    # ).to_sdf()
    # pass

    # Should this also be a component class?
    # Or use ev.apply_schema_migration()?
    # NOTE: Migrations could/should be applied separately to different namespaces correct?
    evolve_catalog_schema(
        spark=ev.spark,
        migrations_dir_path=Path(__file__).parent,
        catalog_name=catalog_name,
        namespace=namespace
    )

    # We should be able to read in the tables from the local evaluation.
    options = {
        "header": "true",
        "ignoreMissingFiles": "true"
    }

    ev.set_active_catalog("remote")

    units_table_dir = e4_evaluation_path / "dataset" / "units"
    units_table = ev.units
    schema = units_table.schema_func().to_structtype()
    units_sdf = ev.spark.read.format(units_table.format).options(**options).load(units_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=units_sdf,
        target_table="units",
        namespace=namespace
    )

    configuration_table_dir = e4_evaluation_path / "dataset" / "configurations"
    configuration_table = ev.configurations
    schema = configuration_table.schema_func().to_structtype()
    configuration_sdf = ev.spark.read.format(configuration_table.format).options(**options).load(configuration_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=configuration_sdf,
        target_table="configurations",
        namespace=namespace
    )

    variables_table_dir = e4_evaluation_path / "dataset" / "variables"
    variables_table = ev.variables
    schema = variables_table.schema_func().to_structtype()
    variables_sdf = ev.spark.read.format(variables_table.format).options(**options).load(variables_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=variables_sdf,
        target_table="variables",
        namespace=namespace
    )

    attributes_table_dir = e4_evaluation_path / "dataset" / "attributes"
    attributes_table = ev.attributes
    schema = attributes_table.schema_func().to_structtype()
    attributes_sdf = ev.spark.read.format(attributes_table.format).options(**options).load(attributes_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=attributes_sdf,
        target_table="attributes",
        namespace=namespace
    )

    locations_table_dir = e4_evaluation_path / "dataset" / "locations"
    locations_table = ev.locations
    schema = locations_table.schema_func().to_structtype()
    locations_sdf = ev.spark.read.format(locations_table.format).options(**options).load(locations_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=locations_sdf,
        target_table="locations",
        namespace=namespace
    )

    location_attrs_table_dir = e4_evaluation_path / "dataset" / "location_attributes"
    location_attrs_table = ev.location_attributes
    schema = location_attrs_table.schema_func().to_structtype()
    location_attrs_sdf = ev.spark.read.format(location_attrs_table.format).options(**options).load(location_attrs_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=location_attrs_sdf,
        target_table="location_attributes",
        namespace=namespace
    )

    primary_timeseries_table_dir = e4_evaluation_path / "dataset" / "primary_timeseries"
    primary_timeseries_table = ev.primary_timeseries
    schema = primary_timeseries_table.schema_func().to_structtype()
    primary_timeseries_sdf = ev.spark.read.format(primary_timeseries_table.format).options(**options).load(primary_timeseries_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=primary_timeseries_sdf,
        target_table="primary_timeseries",
        namespace=namespace
    )

    secondary_timeseries_table_dir = e4_evaluation_path / "dataset" / "secondary_timeseries"
    secondary_timeseries_table = ev.secondary_timeseries
    schema = secondary_timeseries_table.schema_func().to_structtype()
    secondary_timeseries_sdf = ev.spark.read.format(secondary_timeseries_table.format).options(**options).load(secondary_timeseries_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=secondary_timeseries_sdf,
        target_table="secondary_timeseries",
        namespace=namespace
    )

    joined_timeseries_table_dir = e4_evaluation_path / "dataset" / "joined_timeseries"
    joined_timeseries_table = ev.joined_timeseries
    schema = joined_timeseries_table.schema_func().to_structtype()
    joined_timeseries_sdf = ev.spark.read.format(joined_timeseries_table.format).options(**options).load(joined_timeseries_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=joined_timeseries_sdf,
        target_table="joined_timeseries",
        namespace=namespace
    )

    location_crosswalk_table_dir = e4_evaluation_path / "dataset" / "location_crosswalks"
    location_crosswalk_table = ev.location_crosswalks
    schema = location_crosswalk_table.schema_func().to_structtype()
    location_crosswalk_sdf = ev.spark.read.format(location_crosswalk_table.format).options(**options).load(location_crosswalk_table_dir.as_posix(), schema=schema)
    ev.write.to_warehouse(
        source_data=location_crosswalk_sdf,
        target_table="location_crosswalks",
        namespace=namespace
    )

    pass


if __name__ == "__main__":
    main()