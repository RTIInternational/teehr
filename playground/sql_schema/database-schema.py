import duckdb

conn = duckdb.connect()

conn.sql("""
CREATE TABLE IF NOT EXISTS units (
         name VARCHAR PRIMARY KEY,
         long_name VARCHAR,
         -- symbol VARCHAR,
         -- aliases VARCHAR[],
);
COPY units FROM 'playground/domain_examples/units.csv';
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS configurations (
         name VARCHAR PRIMARY KEY,
         type VARCHAR,
         description VARCHAR
);
COPY configurations FROM 'playground/domain_examples/configurations.csv';
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS variables (
         name VARCHAR PRIMARY KEY,
         long_name VARCHAR
         -- interval_seconds INTEGER,
);
COPY variables FROM 'playground/domain_examples/variables.csv';
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS attributes (
         name VARCHAR PRIMARY KEY,
         unit_name VARCHAR,
         type VARCHAR,
         FOREIGN KEY (unit_name) REFERENCES units (name)
);
INSERT INTO attributes VALUES ('drainage_area_km2', 'km2', 'continuous');
INSERT INTO attributes VALUES ('mean_daily_flow_cms', 'cms', 'continuous');
""")

conn.sql("""
INSTALL spatial;
LOAD spatial;
CREATE TABLE IF NOT EXISTS locations (
         id VARCHAR PRIMARY KEY,
         name VARCHAR,
         geometry GEOMETRY
);
INSERT INTO locations VALUES ('p-1', 'gage-A', 'POINT(1 1)');
INSERT INTO locations VALUES ('p-2', 'gage-B', 'POINT(2 2)');
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS location_crosswalks (
         primary_location_id VARCHAR,
         secondary_location_id VARCHAR UNIQUE,
         PRIMARY KEY (secondary_location_id, primary_location_id),
         FOREIGN KEY (primary_location_id) REFERENCES locations (id)
);
INSERT INTO location_crosswalks VALUES ('p-1', 's-1');
INSERT INTO location_crosswalks VALUES ('p-2', 's-2');
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS primary_timeseries (
    reference_time DATETIME,
    value_time DATETIME,
    configuration_name VARCHAR,
    unit_name VARCHAR,
    variable_name VARCHAR,
    value FLOAT,
    location_id VARCHAR,
    FOREIGN KEY (configuration_name) REFERENCES configurations (name),
    FOREIGN KEY (unit_name) REFERENCES units (name),
    FOREIGN KEY (variable_name) REFERENCES variables (name),
    FOREIGN KEY (location_id) REFERENCES locations (id)
);
""")

conn.sql("""
INSERT INTO primary_timeseries VALUES ('2024-01-01 12:00:00', '2024-01-01 12:00:00', 'usgs_observations', 'cms', 'streamflow_daily_mean', 0.12, 'p-1');
INSERT INTO primary_timeseries VALUES ('2024-01-02 12:00:00', '2024-01-01 12:00:00', 'usgs_observations', 'cms', 'streamflow_daily_mean', 2.1, 'p-1');
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS secondary_timeseries (
    reference_time DATETIME,
    value_time DATETIME,
    configuration_name VARCHAR,
    unit_name VARCHAR,
    variable_name VARCHAR,
    value FLOAT,
    location_id VARCHAR,
    FOREIGN KEY (configuration_name) REFERENCES configurations (name),
    FOREIGN KEY (unit_name) REFERENCES units (name),
    FOREIGN KEY (variable_name) REFERENCES variables (name),
    FOREIGN KEY (location_id) REFERENCES location_crosswalks (secondary_location_id)
);
""")

conn.sql("""
INSERT INTO secondary_timeseries VALUES ('2024-01-01 12:00:00', '2024-01-01 12:00:00', 'nwm30_retro', 'cms', 'streamflow_daily_mean', 0.2, 's-1');
INSERT INTO secondary_timeseries VALUES ('2024-01-02 12:00:00', '2024-01-01 12:00:00', 'nwm30_retro', 'cms', 'streamflow_daily_mean', 2.3, 's-1');
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS location_attributes(
         location_id VARCHAR,
         attribute_name VARCHAR PRIMARY KEY,
         value VARCHAR,
         FOREIGN KEY (attribute_name) REFERENCES attributes (name),
         FOREIGN KEY (location_id) REFERENCES locations (id)
);
""")

conn.sql("""
CREATE TABLE IF NOT EXISTS joined_timeseries(
    reference_time DATETIME,
    value_time DATETIME,
    configuration_name VARCHAR,
    unit_name VARCHAR,
    variable_name VARCHAR,
    primary_location_id VARCHAR,
    secondary_location_id VARCHAR,
    primary_value FLOAT,
    secondary_value FLOAT,
    FOREIGN KEY (configuration_name) REFERENCES configurations (name),
    FOREIGN KEY (unit_name) REFERENCES units (name),
    FOREIGN KEY (variable_name) REFERENCES variables (name),
    FOREIGN KEY (secondary_location_id)
         REFERENCES location_crosswalks (secondary_location_id),
    FOREIGN KEY (primary_location_id) REFERENCES locations(id)
);
""")

units = conn.sql("SELECT * FROM units;")
print(units)

configurations = conn.sql("SELECT * FROM configurations;")
print(configurations)

variables = conn.sql("SELECT * FROM variables;")
print(variables)

attributes = conn.sql("SELECT * FROM attributes;")
print(attributes)

location_crosswalks = conn.sql("SELECT * FROM location_crosswalks;")
print(location_crosswalks)

locations = conn.sql("SELECT * FROM locations;")
print(locations)

primary_timeseries = conn.sql("SELECT * FROM primary_timeseries;")
print(primary_timeseries)

secondary_timeseries = conn.sql("SELECT * FROM secondary_timeseries;")
print(secondary_timeseries)

location_attributes = conn.sql("SELECT * FROM location_attributes;")
print(location_attributes)

joined_timeseries = conn.sql("SELECT * FROM joined_timeseries;")
print(joined_timeseries)

conn.close()