This directory contains a TEEHR Evaluation template.
It is intended to be used as a starting point for starting a new evaluation.

The following directories should contain CSV files of "domain tables" that are used to
validate fields in other "data tables".
- attributes
- units
- configurations
- variables

The following "table table" directories should contain Apache Parquet files that contain
majority of the data for the evaluation (e.g., timeseries, locations, etc.)
- locations
- location_crosswalks
- location_attributes
- primary_timeseries
- secondary_timeseries
- joined_timeseries