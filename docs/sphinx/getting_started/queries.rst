.. _queries:

Queries
=======

The TEEHR data library provides tools for querying data from the cached parquet files and for generating metrics.  This includes:

* Get Timeseries
* Get Timeseries Characteristics
* Get Joined Timeseries
* Get Metrics

Get Timeseries
--------------
This feature simply applies filters to the timeseries tables and returns the requested timeseries.


Get Timeseries Characteristics
------------------------------
This feature returns simple summary statistics on the requested timeseries.


Get Joined Timeseries
---------------------
This feature joined two different tables of timeseries together based on location and time, applies filters and returns the paired timeseries

.. figure:: ../../images/joined_timeseries.png
   :scale: 75%

   Getting joined timeseries schematic.

Metrics
-------
This feature starts by joining the timeseries as described above, then the timeseries are grouped to create populations, then the requested metrics are calculated.

.. figure:: ../../images/metrics.png
   :scale: 70%

   Calculating metrics schematic.