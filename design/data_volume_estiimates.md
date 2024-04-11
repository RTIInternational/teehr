This document summarizes some estimates of different data volumes we may encounter in different scenarios for planning purposes:

The most memory intensive part of our current pipeline is the joining of the primary timeseries to the secondary.

## 40-yr Restrospective Streamflow
This scenario is representative of a large but not unreasonable CONUS protocol to evaluate stream flow predictions.  One years' worth of hourly data at the 7500 usgs sites we are using for CONUS scale evaluations is approximately 1 GB on disk as a parquet file.  A single baseline or study that uses an hourly timestep is also a similar size.

For reference, opening with pandas (not setting column type to categorical) uses about 15.8 GB of memory. Switching the `location_id`, `variable_name`, `measurement_unit` and `configuration` to type `category` makes the dataframe take only 1.5 GB in memory.  However, we do not actually read with Pandas but rather with DuckDB for processing the analytics.  Reading into a Arrow table with DuckDB, the file takes up about 4.5 GB in memory.  So, the exact numbers to use for evaluating this problem are not clear, but best case is around 60 GB in memory and worst is around 640 GB in memory per study, to load the entire dataset in memory.  If we are smart, however, we won't do this.  Query engines like DuckDB and others will only read in the data that is needed and can read in chunks to limit overall memory use/requirements.

| Data       | Size on Disk | Size in Memory       | Total on Disk | Total in Memory      |
|------------|--------------|----------------------|---------------|----------------------|
| Primary    | 1 GB/yr      | 1.5 - 16 GB/yr       | 40 GB         | 60 - 640 GB          |
| Secondary  | 1 GB/yr      | 1.5 - 16 GB/yr       | 40 GB         | 60 - 640 GB          |
| Study 1    | 1 GB/yr      | 1.5 - 16 GB/yr       | 40 GB         | 60 - 640 GB          |
| Study 2    | 1 GB/yr      | 1.5 - 16 GB/yr       | 40 GB         | 60 - 640 GB          |

