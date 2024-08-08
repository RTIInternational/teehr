"""
This simple script converts tes *.csv files to parquet files.

To run this:

```bash
$ cd tests/data/test_study/timeseries/
$ python convert.py
```

"""
import pandas as pd

print("test_short_fcast.csv")
df = pd.read_csv(
    "tests/data/test_study/timeseries/test_short_fcast.csv",
    parse_dates=['reference_time', 'value_time']
)
df.to_parquet("tests/data/test_study/timeseries/test_short_fcast.parquet")
print(df.info())

print("test_short_obs.csv")
df = pd.read_csv(
    "tests/data/test_study/timeseries/test_short_obs.csv",
    parse_dates=['reference_time', 'value_time']
)
df.to_parquet("tests/data/test_study/timeseries/test_short_obs.parquet")
print(df.info())

print("test_short_dup_obs.csv")
df = pd.read_csv(
    "tests/data/test_study/timeseries/test_short_dup_obs.csv",
    parse_dates=['reference_time', 'value_time']
)
df.to_parquet("tests/data/test_study/timeseries/test_short_dup_obs.parquet")
print(df.info())

print("test_short_fcast_2.csv")
df = pd.read_csv(
    "tests/data/test_study/timeseries/test_short_fcast_2.csv",
    parse_dates=['reference_time', 'value_time']
)
df.to_parquet("tests/data/test_study/timeseries/test_short_fcast_2.parquet")
print(df.info())

print("test_short_obs_2.csv")
df = pd.read_csv(
    "tests/data/test_study/timeseries/test_short_obs_2.csv",
    parse_dates=['reference_time', 'value_time']
)
df.to_parquet("tests/data/test_study/timeseries/test_short_obs_2.parquet")
print(df.info())
