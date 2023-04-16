"""
This simple script converts `test_short_fcast.csv` and `test_short_obs.csv` 
to parquet files.

To run this:
```bash
$ cd cd tests/data/test_study/timeseries/
$ python convert.py

```
"""
import pandas as pd

print(f"test_short_fcast.csv")
df = pd.read_csv("test_short_fcast.csv", parse_dates=['reference_time', 'value_time'])
df.to_parquet("test_short_fcast.parquet")
print(df.info())

print(f"test_short_obs.csv")
df = pd.read_csv("test_short_obs.csv", parse_dates=['reference_time', 'value_time'])
df.to_parquet("test_short_obs.parquet")
print(df.info())