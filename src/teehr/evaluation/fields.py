"""Fields class for querying field enums."""
# from teehr.querying.field_enums import (
#     get_location_fields,
#     get_configuration_fields,
#     get_unit_fields,
#     get_variable_fields,
#     get_attribute_fields,
#     get_location_attribute_fields,
#     get_location_crosswalk_fields,
#     get_timeseries_fields,
#     get_joined_timeseries_fields
# )
# from teehr.models.dataset.table_enums import (
#     ConfigurationFields,
#     UnitFields,
#     VariableFields,
#     AttributeFields,
#     LocationFields,
#     LocationAttributeFields,
#     LocationCrosswalkFields,
#     TimeseriesFields,
#     JoinedTimeseriesFields
# )


# class Fields:
#     """Fields class for querying field enums."""

#     def __init__(self, eval):
#         """Initialize the Fields class."""
#         self._spark = eval.spark
#         self._joined_timeseries_dir = eval.joined_timeseries_dir

#     def get_location_fields(self) -> LocationFields:
#         """Get the location fields."""
#         return get_location_fields()

#     def get_configuration_fields(self) -> ConfigurationFields:
#         """Get the configuration fields."""
#         return get_configuration_fields()

#     def get_unit_fields(self) -> UnitFields:
#         """Get the unit fields."""
#         return get_unit_fields()

#     def get_variable_fields(self) -> VariableFields:
#         """Get the variable fields."""
#         return get_variable_fields()

#     def get_attribute_fields(self) -> AttributeFields:
#         """Get the attribute fields."""
#         return get_attribute_fields()

#     def get_location_attribute_fields(self) -> LocationAttributeFields:
#         """Get the location attribute fields."""
#         return get_location_attribute_fields()

#     def get_location_crosswalk_fields(self) -> LocationCrosswalkFields:
#         """Get the location crosswalk fields."""
#         return get_location_crosswalk_fields()

#     def get_timeseries_fields(self) -> TimeseriesFields:
#         """Get the timeseries fields."""
#         return get_timeseries_fields()

#     def get_joined_timeseries_fields(self) -> JoinedTimeseriesFields:
#         """Get the joined timeseries fields."""
#         return get_joined_timeseries_fields(
#             self._spark,
#             self._joined_timeseries_dir
#         )
