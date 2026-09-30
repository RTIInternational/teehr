CREATE TABLE IF NOT EXISTS location_id_aliases (
    location_id STRING,
    location_id_alias STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    properties MAP<STRING, STRING>
) USING iceberg
