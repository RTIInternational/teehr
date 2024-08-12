"""
This simple script converts `crosswalk.csv` and `gages.geojson`
to parquet files.

To run this:
```bash
$ cd cd tests/data/test_study/geo/
$ python convert.py

```
"""
import pandas as pd
import geopandas as gpd

print(f"crosswalk.csv")
df = pd.read_csv("crosswalk.csv")
df.to_parquet("crosswalk.parquet")

print(f"gages.geojson")
gdf = gpd.read_file("gages.geojson")
gdf.to_parquet("gages.parquet")

print("test_attr_ecoregion.csv")
df = pd.read_csv("test_attr_ecoregion.csv")
df.to_parquet("test_attr_ecoregion.parquet")

print("test_attr_2yr_discharge.csv")
df = pd.read_csv("test_attr_2yr_discharge.csv")
df.to_parquet("test_attr_2yr_discharge.parquet")

print("test_attr_drainage_area_km2.csv")
df = pd.read_csv("test_attr_drainage_area_km2.csv")
df.to_parquet("test_attr_drainage_area_km2.parquet")

print("test_attr_drainage_area_mi2.csv")
df = pd.read_csv("test_attr_drainage_area_mi2.csv")
df.to_parquet("test_attr_drainage_area_mi2.parquet")