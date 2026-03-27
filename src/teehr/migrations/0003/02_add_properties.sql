ALTER TABLE locations ADD COLUMNS (
    properties MAP<STRING, STRING> comment 'Additional properties for the location as key-value pairs'
);

ALTER TABLE location_crosswalks ADD COLUMNS (
    properties MAP<STRING, STRING> comment 'Additional properties for the location as key-value pairs'
);

ALTER TABLE location_attributes ADD COLUMNS (
    properties MAP<STRING, STRING> comment 'Additional properties for the location as key-value pairs'
);