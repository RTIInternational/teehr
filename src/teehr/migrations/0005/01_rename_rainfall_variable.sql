UPDATE variables
SET name = 'rainfall_hourly_mean',
    long_name = 'Hourly Mean Rainfall Rate'
WHERE name = 'rainfall_hourly_rate'

UPDATE primary_timeseries
SET variable_name = 'rainfall_hourly_mean'
WHERE variable_name = 'rainfall_hourly_rate'

UPDATE secondary_timeseries
SET variable_name = 'rainfall_hourly_mean'
WHERE variable_name = 'rainfall_hourly_rate'
