"""Functions for adding user defined fields to joined timeseries data.

This file gets dynamically imported to the TEEHR
create_joined_timeseries_dataset() function and is used to add user
defined fields when a user runs ev.create_joined_timeseries().

Users can modify this script to add or remove user defined fields to
the joined timeseries data.

WARNING: Do not change the name of this file or the functions it contains.
"""

from pyspark.sql import DataFrame
import logging
from teehr import RowLevelCalculatedFields as rcf

logger = logging.getLogger(__name__)


def add_user_defined_fields(
    joined_df: DataFrame
):
    """Add user defined fields to the joined timeseries data.

    Do not change the name of this function.

    Parameters
    ----------
    joined_df : DataFrame

    Returns
    -------
    DataFrame
        The joined timeseries data with user defined fields.
    """
    logger.info("Adding user defined fields")

    # Add a month field to the joined timeseries data
    logger.info("Adding month from date")

    month = rcf.Month()
    year = rcf.Year()
    water_year = rcf.WaterYear()
    seasons = rcf.Seasons()

    cfs = [
        month,
        year,
        water_year,
        seasons
    ]

    for cf in cfs:
        joined_df = cf.apply_to(joined_df)

    # Return the joined timeseries data with user defined fields
    return joined_df