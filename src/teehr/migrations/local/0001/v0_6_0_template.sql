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
    description STRING,
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
    location_id STRING
) USING iceberg PARTITIONED BY (configuration_name, variable_name);

CREATE TABLE IF NOT EXISTS secondary_timeseries (
    reference_time TIMESTAMP,
    value_time TIMESTAMP,
    configuration_name STRING,
    unit_name STRING,
    variable_name STRING,
    value FLOAT,
    location_id STRING,
    member STRING
) USING iceberg PARTITIONED BY (configuration_name, variable_name);

CREATE TABLE IF NOT EXISTS joined_timeseries (
    reference_time TIMESTAMP,
    value_time TIMESTAMP,
    configuration_name STRING,
    unit_name STRING,
    variable_name STRING,
    primary_value FLOAT,
    secondary_value FLOAT,
    primary_location_id STRING,
    secondary_location_id STRING,
    member STRING
) USING iceberg PARTITIONED BY (configuration_name, variable_name);

CREATE TABLE IF NOT EXISTS location_attributes(
    location_id STRING,
    attribute_name STRING,
    value STRING
) USING iceberg;

-- Should these be if not exists?
INSERT INTO units VALUES
    ("m^3/s", "Cubic Meters Per Second"),
    ("ft^3/s", "Cubic Feet Per Second"),
    ("km^2", "Square Kilometers"),
    ("mm/s", "Millimeters Per Second");

INSERT INTO variables VALUES
    ("streamflow", "Hourly Instantaneous Streamflow"),
    ("streamflow_hourly_inst", "Hourly Instantaneous Streamflow"),
    ("streamflow_daily_mean", "Daily Mean Streamflow"),
    ("rainfall_hourly_rate", "Hourly Rainfall Rate");