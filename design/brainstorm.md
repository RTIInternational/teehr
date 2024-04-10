- Switch to class based approach for all queries.  When initializing we point to sources of data, unjoined parquet files, joined parquet files, database, possible external.

- Build transactional system database and standardize the "string" variables (variable, measurement_unit, configuration)

- Might need a queue and workers ro respond to web api calls

- Maybe we should separate calls for each metric to simplify queries, at the expense of efficiency.  This would not be as big a deal when operating on joined timeseries.

- We should maybe consider requiring the database approach.  If the databsae is going to be too big we can offload the joined, attributed and user defined function tables to parquet in S3 for greater parallelization of metrics.
    - MPP could be done with Dask or some other scalable MPP option (Apache...)

- The most expensive part of the current queries is the joining of the primary timeseries (observations) to the secondary timeseries (simulated).  Since all metrics required joining the two timeseries at a minimum, this seems like a reasonable step to be handled in pre-processing.  This step is also the most challenging to parallelize across multiple nodes due to the high amount of shuffling of data between nodes.  The protocol can specify the preprocessing that is required for a given protocol.  A script can be written to execute the preprocessing, for now, with potential to make it an automated process that reads from the protocol doc in the near future.
  - Wondering if preprocessing in terms of spatial chunks (ex. lat/lon extent) rather than temporal chunks would allow for more horizontal scalability.  Mabye we could scale the join operation horizontally if we first had the primary and secondary locations grouped into zones (this would require lat/lon)?

- We could pass the timeseries data through a relational database with schema (duckdb, sqlite, postgres, etc.), foreign keys, domains, etc., then export to parquet.  This would clean it.  And would ensure consistency.  Or we could write some queries to validate the data before saving to parquet.


- We have several methods to do the processing with a few additional ones being considered.  To document:
    1) We have using DuckBD against the raw parquet files.  This has been the most prominent method so far and does all the processing on the fly.  This includes joining the primary to the secondary, grouping, filtering and finally calculating the metrics.  Super flexible, but getting messy.  This is performant up to a point ~
    2) We have the Pandas methods that mimic the DuckDB methods but have limited ability to operate on large datasets.  These are used more for testing that the DuckDB methods are generating the correct answer (or at least a consistent answer).
    3) We have the DuckDB database method where we join the primary to the secondary timeseries, add the attributes, add user defined fields, and then execute queries against that database (`get_metrics()`, `get_timeseries()`, `get_unique_fields()`, etc).  This is much more performant as the joins are done in preprocessing.
    4) This is not technically part of the current tool set, but the option exists (manually) to export the joined and attributed timeseries to parquet as a "wide table".  These "wide table" format parquet files can be queried to have improved performance while still keeping data in parquet files in S3.  This has the benefit of technically being able to be queried using DuckDB or some other distributed (massively parallel processing) like Dask or many others.

- It is probably worth looking into Polars as a performant (like DuckDB as I understand it) but more Pandas like from a UI perspective.

### Pain Points
- Scaling with larger datasets
  - Seems like the duckdb database approach is great for "smaller" data (10's of GB?), but potentially not feasible as data size increases (100's of GB?)
  - Do we need two different approaches?
- Joins
  - As noted above, these are expensive and hard to scale
  - Can we do these in spatial chunks?
- Data management
  - We do not currently have a mechanism to keep track of a users data (Data Catalog)
  - We do not currently have much data validation - could use a transactional database for this as noted above?
- Database size
  - Probably not feasible to use the duckdb persistent database for larger datasets (NWM retro)

### Components

- Fetching External data
- Preprocessing
  - Ensure consistency and validate the data
  - Loading into TEEHR data model and data storage format
  - parquet files
    - duckdb persistent database
    - zarr
    - local and/or s3
  - We will likely have two types of data storage
    - Long-term (bigger data): Pre-processed data ready for people to use like NWM retro data joined to USGS gage data or summarized to some catchments, etc. Can we pre-calculate anything here? (annual peak flow, flow duration curve slope, etc?)
    - Short-term (smaller): Related to a users specific analysis
    - Do we need two different approaches for each?  Seems like duckdb persistent database is excellent for "smaller" datasets.
  - Join primary and secondary
  - Summarizing gridded data to polygons
- User-defined fields
- Metric Calculations
  - Comparing primary to secondary timeseries
  - Helps answer the question, "which model (secondary timeseries) performs best?"