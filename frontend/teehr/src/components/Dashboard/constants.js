export const defaultFormState = {
  selectedDataset: "",
  selectedMetrics: [],
  selectedGroupByFields: [],
  selectedFilters: [],
  includeSpatialData: true,
};

export const nonListFields = [
  "reference_time",
  "value_time",
  "secondary_value",
  "primary_value",
  "absolute_difference",
  "upstream_area_km2",
  "primary_normalized_discharge",
  "exceed_2yr_recurrence",
  "",
];
