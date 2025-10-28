===============
Data Management
===============

Fetching Data
-------------

Methods for fetching NWM and USGS data from external sources.

.. currentmodule:: teehr.evaluation.fetch

.. autosummary::
   :template: custom-class-template.rst
   :toctree: generated
   :nosignatures:

   Fetch

.. currentmodule:: teehr.evaluation.fetch.Fetch

.. autosummary::
   :nosignatures:

   usgs_streamflow
   nwm_retrospective_points
   nwm_operational_points
   nwm_retrospective_grids
   nwm_operational_grids

Dataset Tables
--------------

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