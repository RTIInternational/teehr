.. _user_guide:

==========
User Guide
==========

This guide provides examples of using some of TEEHR's core features.

Fetching and Loading Data
-------------------------

TEEHR provides tools for fetching and loading hydrologic data into an efficient storage format (parquet). It currently
supports both operational and retrospective National Water Model data as well as USGS streamflow data. Support for
the Office of Water Prediction's Nextgen data is also in development.

:doc:`NWM Operational Point Data </user_guide/notebooks/loading/point_loading_example>`

:doc:`NWM Operational Gridded Data </user_guide/notebooks/loading/grid_loading_example>`

:doc:`NWM Retrospective Point Data </user_guide/notebooks/loading/load_retrospective>`

:doc:`NWM Retrospective Gridded Data </user_guide/notebooks/loading/load_gridded_retrospective>`

:doc:`USGS Streamflow Data </user_guide/notebooks/loading/usgs_loading>`

Model Evaluation and Visualization with TEEHR
---------------------------------------------

Once the timeseries data conforms to the data model, we can take advantage of TEEHR's capabilities for
quantifying and understanding model performance.  The following example notebooks demonstrate how to
use TEEHR to join timeseries and evaluate model performance through calculation of various metrics.

While mapping and plotting functionality is not yet available in TEEHR, we provide several examples of visualizing
model output using the `holoviews` library.

Note: These notebooks were developed for the 2024 CIROH Developer's Conference and can be also found
here: https://github.com/RTIInternational/teehr-devcon24-workshop

:doc:`Building a Simple TEEHR Dataset </user_guide/notebooks/evaluation/01-ex1-simple>`

:doc:`Building a Joined Database </user_guide/notebooks/evaluation/02-ex2_join_data>`

:doc:`Evaluating and Visualizing Model Output, Ex. 1 </user_guide/notebooks/evaluation/03-ex2_evaluate>`

:doc:`Evaluating and Visualizing Model Output, Ex. 2 </user_guide/notebooks/evaluation/04-ex3_evaluate>`


.. toctree::
   :maxdepth: 2
   :hidden:

   notebooks/loading_examples_index
   notebooks/evaluation_examples_index
   metrics/metrics