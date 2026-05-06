UPDATE variables
SET name = 'rainrate_hourly_mean',
    long_name = 'Hourly Mean Rainfall Rate'
WHERE name = 'rainfall_hourly_mean'

UPDATE primary_timeseries
SET variable_name = 'rainrate_hourly_mean'
WHERE variable_name = 'rainfall_hourly_mean'

UPDATE secondary_timeseries
SET variable_name = 'rainrate_hourly_mean'
WHERE variable_name = 'rainfall_hourly_mean'
