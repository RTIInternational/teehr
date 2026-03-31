ALTER TABLE configurations ADD COLUMNS (
    properties MAP<STRING, STRING> comment 'Additional properties for the configuration as key-value pairs'
);