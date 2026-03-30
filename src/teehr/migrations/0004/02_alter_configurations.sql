ALTER TABLE configurations ADD COLUMNS (
    properties MAP<STRING, STRING> comment 'Additional properties for the location as key-value pairs'
);