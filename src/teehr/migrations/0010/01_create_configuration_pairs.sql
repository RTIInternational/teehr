CREATE TABLE IF NOT EXISTS configuration_pairs (
    primary_configuration_name STRING,
    secondary_configuration_name STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    properties MAP<STRING, STRING>
) USING iceberg
