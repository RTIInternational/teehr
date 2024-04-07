- Switch to class based approach for all queries.  When initializing we point to sources of data, unjoined parquet files, joined parquet files, database, possible external.

- Build transactional system database and standardize the "string" variables (variable, measurement_unit, configuration)

- might need a queue and workers ro reapond to web api calls

- maybe seperate calls for each metric to simplify queries, at the expense of efficiency.  this woild not be as big a deal when operating on joined timeseries.