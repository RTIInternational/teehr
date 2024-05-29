import pandas as pd
import geopandas as gpd

# Convert location dat to parquet
locations = gpd.read_file("tests/data/simple_sim_example/raw/gages.geojson")
print(locations)
locations.to_parquet("tests/data/simple_sim_example/database/geometry/locations.parquet")

# Convert crosswalks
sim_xw = pd.read_csv("tests/data/simple_sim_example/raw/sim-crosswalk.csv")
print(sim_xw)
sim_xw.to_parquet("tests/data/simple_sim_example/database/crosswalk/sim.parquet")

model_xw = pd.read_csv("tests/data/simple_sim_example/raw/model-crosswalk.csv")
print(model_xw)
model_xw.to_parquet("tests/data/simple_sim_example/database/crosswalk/model.parquet")

# Convert attributes
attr1 = pd.read_csv("tests/data/simple_sim_example/raw/gage_attr_2yr_discharge.csv")
print(attr1)
attr1.to_parquet("tests/data/simple_sim_example/database/attribute/2yr_discharge.parquet")

attr2 = pd.read_csv("tests/data/simple_sim_example/raw/gage_attr_drainage_area_km2.csv")
print(attr2)
attr2.to_parquet("tests/data/simple_sim_example/database/attribute/drainage_area.parquet")

attr3 = pd.read_csv("tests/data/simple_sim_example/raw/gage_attr_ecoregion.csv")
print(attr3)
attr3.to_parquet("tests/data/simple_sim_example/database/attribute/ecoregion.parquet")

# Convert timeseries
