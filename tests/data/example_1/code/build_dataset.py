"""
This file contains an example of how to build a simple TEEHR dataset.

The input data is all CSV and GeoJSON files.  This is intended to be
the simplest example of how TEEHR can be used.
"""

import pandas as pd
import geopandas as gpd

# Setup database paths
from pathlib import Path
from teehr.classes.duckdb_database import DuckDBDatabase
from teehr.classes.duckdb_joined_parquet import DuckDBJoinedParquet


RAW_DATA_FILEPATH = Path("tests/data/simple_sim_example/raw")

# define the base TEEHR directory location
TEEHR_BASE = Path(Path.home(), "teehr/example_1/teehr_base")
TEEHR_BASE = Path("tests/data/simple_sim_example/teehr_base")

# create folders for each type of TEEHR 'table'
PRIMARY_FILEPATH = Path(TEEHR_BASE, 'primary')
SECONDARY_FILEPATH = Path(TEEHR_BASE, 'secondary')
CROSSWALK_FILEPATH = Path(TEEHR_BASE, 'crosswalk')
GEOMETRY_FILEPATH = Path(TEEHR_BASE, 'geometry')
ATTRIBUTE_FILEPATH = Path(TEEHR_BASE, 'attribute')
JOINED_FILEPATH = Path(TEEHR_BASE, 'joined')
DB_FILEPATH = Path(TEEHR_BASE, 'teehr.db')

PRIMARY_FILEPATH.mkdir(exist_ok=True, parents=True)
SECONDARY_FILEPATH.mkdir(exist_ok=True, parents=True)
CROSSWALK_FILEPATH.mkdir(exist_ok=True, parents=True)
GEOMETRY_FILEPATH.mkdir(exist_ok=True, parents=True)
ATTRIBUTE_FILEPATH.mkdir(exist_ok=True, parents=True)
JOINED_FILEPATH.mkdir(exist_ok=True, parents=True)

# Convert location dat to parquet
locations = gpd.read_file(Path(RAW_DATA_FILEPATH, "gages.geojson"))
print(locations)
locations.to_parquet(Path(GEOMETRY_FILEPATH, "locations.parquet"))

# Convert crosswalks
sim_xw = pd.read_csv(Path(RAW_DATA_FILEPATH, "sim-crosswalk.csv"))
print(sim_xw)
sim_xw.to_parquet(Path(CROSSWALK_FILEPATH, "sim-crosswalk.parquet"))

baseline_xw = pd.read_csv(Path(RAW_DATA_FILEPATH, "baseline-crosswalk.csv"))
print(baseline_xw)
baseline_xw.to_parquet(Path(CROSSWALK_FILEPATH, "baseline-crosswalk.parquet"))

# Convert attributes
attr1 = pd.read_csv(Path(RAW_DATA_FILEPATH, "gage_attr_2yr_discharge.csv"))
print(attr1)
attr1.to_parquet(Path(ATTRIBUTE_FILEPATH, "2yr_discharge.parquet"))

attr2 = pd.read_csv(Path(RAW_DATA_FILEPATH, "gage_attr_drainage_area_km2.csv"))
print(attr2)
attr2.to_parquet(Path(ATTRIBUTE_FILEPATH, "drainage_area.parquet"))

attr3 = pd.read_csv(Path(RAW_DATA_FILEPATH, "gage_attr_ecoregion.csv"))
print(attr3)
attr3.to_parquet(Path(ATTRIBUTE_FILEPATH, "ecoregion.parquet"))

# Convert timeseries
obs = pd.read_csv(Path(RAW_DATA_FILEPATH, "obs.csv"))
print(obs)

# Add the other columns required for TEEHR
obs['configuration'] = 'usgs'
obs['variable_name'] = 'streamflow_daily_mean'
obs['measurement_unit'] = 'cms'
obs['reference_time'] = None
# Reference_time column must be cast as type datetime64[ns] if set to None
obs['reference_time'] = obs['reference_time'].astype('datetime64[ns]')
print(obs)
obs.to_parquet(Path(PRIMARY_FILEPATH, "obs.parquet"))


baseline_ts = pd.read_csv(Path(RAW_DATA_FILEPATH, "baseline.csv"))
print(baseline_ts)
# Add the other columns required for TEEHR
baseline_ts['configuration'] = 'modeled'
baseline_ts['variable_name'] = 'streamflow_daily_mean'
baseline_ts['measurement_unit'] = 'cms'
baseline_ts['reference_time'] = None
# Reference_time column must be cast as type datetime64[ns] if set to None
baseline_ts['reference_time'] = (
    baseline_ts['reference_time'].astype('datetime64[ns]')
)
print(baseline_ts)
baseline_ts.to_parquet(Path(SECONDARY_FILEPATH, "baseline.parquet"))

sim_ts = pd.read_csv(Path(RAW_DATA_FILEPATH, "sim.csv"))
print(baseline_ts)
# Add the other columns required for TEEHR
sim_ts['configuration'] = 'sim'
sim_ts['variable_name'] = 'streamflow_daily_mean'
sim_ts['measurement_unit'] = 'cms'
sim_ts['reference_time'] = None
# Reference_time column must be cast as type datetime64[ns] if set to None
sim_ts['reference_time'] = (
    baseline_ts['reference_time'].astype('datetime64[ns]')
)
print(sim_ts)
sim_ts.to_parquet(Path(SECONDARY_FILEPATH, "sim.parquet"))

PRIMARY_FILEPATH = f"{PRIMARY_FILEPATH}/**/*.parquet"
SECONDARY_FILEPATH = f"{SECONDARY_FILEPATH}/**/*.parquet"
CROSSWALK_FILEPATH = f"{CROSSWALK_FILEPATH }/**/*.parquet"
GEOMETRY_FILEPATH = f"{GEOMETRY_FILEPATH }/**/*.parquet"
ATTRIBUTE_FILEPATH = f"{ATTRIBUTE_FILEPATH}/**/*.parquet"

# Join the data
if DB_FILEPATH.is_file():
    DB_FILEPATH.unlink()

db = DuckDBDatabase(DB_FILEPATH)

# Insert the timeseries data
db.insert_joined_timeseries(
    primary_filepath=PRIMARY_FILEPATH,
    secondary_filepath=SECONDARY_FILEPATH,
    crosswalk_filepath=CROSSWALK_FILEPATH,
    drop_added_fields=True,
)

# Insert geometry
db.insert_geometry(GEOMETRY_FILEPATH)

# Insert attributes
db.insert_attributes(ATTRIBUTE_FILEPATH)

db.query(f"""
    COPY (
        SELECT *
        FROM joined_timeseries
        ORDER BY configuration, primary_location_id, value_time
    )
   TO '{JOINED_FILEPATH}/joined.parquet' (FORMAT PARQUET)
""")

JOINED_FILEPATH = f"{JOINED_FILEPATH}/**/*.parquet"

db = DuckDBJoinedParquet(JOINED_FILEPATH, GEOMETRY_FILEPATH)

jts = db.get_joined_timeseries(
    filters=[
        {
            "column": "primary_location_id",
            "operator": "=",
            "value": "gage-A"
        },
    ],
    order_by=["configuration", "primary_location_id", "value_time"],
)
print(jts)

metrics = db.get_metrics(
    group_by=["primary_location_id", "configuration"],
    order_by=["primary_location_id", "configuration"],
    include_metrics="all"
)
print(metrics)
