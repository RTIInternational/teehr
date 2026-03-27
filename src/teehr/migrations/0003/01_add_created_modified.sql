ALTER TABLE units ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the unit was created',
    updated_at TIMESTAMP comment 'Timestamp when the unit was last updated'
);

ALTER TABLE configurations ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the configuration was created',
    updated_at TIMESTAMP comment 'Timestamp when the configuration was last updated'
);

ALTER TABLE variables ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the variable was created',
    updated_at TIMESTAMP comment 'Timestamp when the variable was last updated'
);

ALTER TABLE attributes ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the attribute was created',
    updated_at TIMESTAMP comment 'Timestamp when the attribute was last updated'
);

ALTER TABLE locations ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the location was created',
    updated_at TIMESTAMP comment 'Timestamp when the location was last updated'
);

ALTER TABLE location_crosswalks ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the location crosswalk was created',
    updated_at TIMESTAMP comment 'Timestamp when the location crosswalk was last updated'
);

ALTER TABLE location_attributes ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the location attribute was created',
    updated_at TIMESTAMP comment 'Timestamp when the location attribute was last updated'
);

ALTER TABLE primary_timeseries ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the primary timeseries was created',
    updated_at TIMESTAMP comment 'Timestamp when the primary timeseries was last updated'
);

ALTER TABLE secondary_timeseries ADD COLUMNS (
    created_at TIMESTAMP comment 'Timestamp when the secondary timeseries was created',
    updated_at TIMESTAMP comment 'Timestamp when the secondary timeseries was last updated'
);
