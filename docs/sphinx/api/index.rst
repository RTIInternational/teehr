.. _api:

.. currentmodule:: teehr

#############
API reference
#############

This page provides an auto-generated summary of TEEHR's API. For more details
and examples, refer to the User Guide part of the
documentation.


The Evaluation Class
--------------------

The top-level class for interacting with and exploring a TEEHR Evaluation.

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   Evaluation


Creating and Managing an Evaluation
-----------------------------------

Methods for creating, cloning, and configuring an Evaluation.

.. currentmodule:: teehr.Evaluation

.. autosummary::
   :nosignatures:

   clone_template
   list_s3_evaluations
   clone_from_s3
   clean_cache
   enable_logging


The Evaluation Dataset
----------------------

Classes for creating, describing, and querying the Evaluation dataset tables.

.. currentmodule:: teehr.evaluation.tables

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   base_table.BaseTable
   unit_table.UnitTable
   variable_table.VariableTable
   attribute_table.AttributeTable
   configuration_table.ConfigurationTable
   location_table.LocationTable
   location_attribute_table.LocationAttributeTable
   location_crosswalk_table.LocationCrosswalkTable
   primary_timeseries_table.PrimaryTimeseriesTable
   secondary_timeseries_table.SecondaryTimeseriesTable
   joined_timeseries_table.JoinedTimeseriesTable

Calculating Metrics
-------------------

Methods related to metric calculations.

.. currentmodule:: teehr.evaluation.metrics

.. autosummary::
   :template: custom-module-template.rst
   :toctree: generated
   :nosignatures:
   :recursive:

   Metrics.query
   Metrics.add_calculated_fields
   Metrics.to_pandas
   Metrics.to_geopandas
   Metrics.to_sdf


Fetching NWM and USGS Data
--------------------------

Methods for fetching NWM and USGS data from external sources.

.. currentmodule:: teehr.evaluation.fetch

.. autosummary::
   :template: custom-module-template.rst
   :toctree: generated
   :nosignatures:

   Fetch.usgs_streamflow
   Fetch.nwm_retrospective_points
   Fetch.nwm_operational_points
   Fetch.nwm_retrospective_grids
   Fetch.nwm_operational_grids


Metric Functions
----------------

Functions for calculating metrics.

.. currentmodule:: teehr.metrics

.. autosummary::
   :toctree: generated
   :template: custom-module-template.rst
   :recursive:
   :nosignatures:

   deterministic_funcs
   signature_funcs
   probabilistic_funcs


Metric and Bootstrap Models
---------------------------

Classes for defining and customizing metrics and bootstrap models.

.. currentmodule:: teehr

.. autosummary::
   :toctree: generated
   :template: custom-class-template.rst
   :nosignatures:

   DeterministicMetrics
   Signatures
   ProbabilisticMetrics
   Bootstrappers


Calculated Field Models
-------------------------

Classes for defining and customizing user-defined field models.

.. currentmodule:: teehr.models.calculated_fields

.. autosummary::
   :toctree: generated
   :template: custom-class-template.rst
   :recursive:
   :nosignatures:


   row_level.RowLevelCalculatedFields
   timeseries_aware.TimeseriesAwareCalculatedFields


Visualization
-------------

Methods for visualizing Evaluation data.

.. currentmodule:: teehr.visualization.dataframe_accessor

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   TEEHRDataFrameAccessor
