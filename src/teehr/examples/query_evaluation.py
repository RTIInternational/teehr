"""Example script for working with an existing evaluation

"""
from teehr import Evaluation
from pathlib import Path

from teehr.models.dataset.filters import LocationFilter
from teehr.models.dataset.filters import FilterOperatorEnum as op

# Set a path to the directory where the evaluation will be created
TEST_STUDY_DIR = Path(Path().home(), "temp", "test_study")

# Create an Evaluation object
eval = Evaluation(dir_path=TEST_STUDY_DIR)

# Enable logging
eval.enable_logging()

# Get the units data
units_df = eval.query.get_units()
print(units_df)

# Get the variables data
variables_df = eval.query.get_variables()
print(variables_df)

# Get the configurations data
configurations_df = eval.query.get_configurations()
print(configurations_df)

# Get the attributes data
attributes_df = eval.query.get_attributes()
print(attributes_df)

# Get the location data
loc_df = eval.query.get_locations()
print(loc_df)

# Get the location_attributes data
loc_attr_df = eval.query.get_location_attributes()
print(loc_attr_df.head())

# Get the location_crosswalks data
loc_crosswalks_df = eval.query.get_location_crosswalks()
print(loc_crosswalks_df.head())

# Get the primary_timeseries data
primary_timeseries_df = eval.query.get_primary_timeseries()
print(primary_timeseries_df.head())

# Get the secondary_timeseries data
secondary_timeseries_df = eval.query.get_secondary_timeseries()
print(secondary_timeseries_df.head())

# Get the joined_timeseries data
joined_timeseries_df = eval.query.get_joined_timeseries()
print(joined_timeseries_df.head())

