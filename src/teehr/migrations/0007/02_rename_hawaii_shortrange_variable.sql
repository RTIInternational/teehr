UPDATE secondary_timeseries
SET variable_name = 'streamflow_15min_inst'
WHERE variable_name = 'streamflow_hourly_inst'
AND configuration_name = 'nwm30_short_range_hawaii'