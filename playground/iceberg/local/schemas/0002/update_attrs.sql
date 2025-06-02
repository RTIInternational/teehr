DROP TABLE IF EXISTS local.db.attributes;

CREATE TABLE IF NOT EXISTS local.db.attributes (
         name STRING,
         type STRING,
         description STRING
) USING iceberg;