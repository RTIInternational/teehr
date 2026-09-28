CREATE TABLE IF NOT EXISTS location_id_aliases (
    primary_location_id STRING,
    alternative_location_id STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    properties MAP<STRING, STRING>
) USING iceberg
