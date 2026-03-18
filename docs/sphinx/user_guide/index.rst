.. _user_guide:

==========
User Guide
==========


This guide provides comprehensive documentation for working with TEEHR. Each section contains
detailed explanations and code examples that you can copy, paste, and adapt for your own use.

Before starting, make sure you have installed TEEHR and its dependencies as described in the
:doc:`Getting Started </getting_started/index>` section.


The Evaluation
--------------
Creating an Evaluation is the first step to working with TEEHR. This section covers the different
evaluation classes, local vs remote access, upgrading existing evaluations, and Apache Spark configuration.

:doc:`evaluation`


Tables
------
The core data model in TEEHR is built around tables - domain tables, timeseries tables, and location data.
This section covers the schema, the Table class and its methods, loading data, and method chaining.

:doc:`tables`


Fetching and Downloading
------------------------
Get data for your Local Evaluation from external sources including the TEEHR warehouse,
USGS, and the National Water Model (NWM).

:doc:`fetching`


Views
-----
Views provide computed, on-the-fly access to joined and transformed data. This section covers
the joined timeseries view, location attributes view, calculated fields, and event detection.

:doc:`views`


Metrics
-------
Calculate performance metrics using the aggregate method. This section covers grouping, filtering,
deterministic and probabilistic metrics, signatures, bootstrapping, and transforms.

:doc:`metrics/metrics`


Generating Data
---------------
Generate synthetic timeseries data including normals and benchmark forecasts.

:doc:`generating`


Visualization
-------------
Create maps and plots from TEEHR query results using HoloViews.

:doc:`visualization`


Additional Resources
--------------------

:doc:`legacy_content/legacy_content`


.. toctree::
   :maxdepth: 1
   :caption: User Guide Sections
   :hidden:

   evaluation
   tables
   views
   metrics/metrics
   fetching
   generating
   visualization


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Additional Resources

   tutorials/joining_timeseries
   legacy_content/legacy_content
