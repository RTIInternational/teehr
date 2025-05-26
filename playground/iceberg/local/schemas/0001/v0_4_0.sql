CREATE TABLE IF NOT EXISTS units (
         name STRING,
         long_name STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS configurations (
         name STRING,
         type STRING,
         description STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS variables (
         name STRING,
         long_name STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS attributes (
         name STRING,
         unit_name STRING,
         type STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS locations (
         id STRING,
         name STRING,
         geometry BINARY
) USING iceberg;

CREATE TABLE IF NOT EXISTS location_crosswalks (
         primary_location_id STRING,
         secondary_location_id STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS primary_timeseries (
    reference_time TIMESTAMP,
    value_time TIMESTAMP,
    configuration_name STRING,
    unit_name STRING,
    variable_name STRING,
    value FLOAT,
    location_id STRING,
    member STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS secondary_timeseries (
    reference_time TIMESTAMP,
    value_time TIMESTAMP,
    configuration_name STRING,
    unit_name STRING,
    variable_name STRING,
    value FLOAT,
    location_id STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS location_attributes(
         location_id STRING,
         attribute_name STRING,
         value STRING
) USING iceberg;
