"""Test duckdb metric queries."""
import pandas as pd
import geopandas as gpd
import numpy as np
import pytest
from pydantic import ValidationError
import teehr.queries.duckdb as tqu
import teehr.queries.pandas as tqp
from pathlib import Path
import duckdb


TEST_STUDY_DIR = Path("tests", "data", "test_study")
PRIMARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "test_short_obs.parquet")
SECONDARY_FILEPATH = Path(TEST_STUDY_DIR, "timeseries", "*_fcast.parquet")
CROSSWALK_FILEPATH = Path(TEST_STUDY_DIR, "geo", "crosswalk.parquet")
GEOMETRY_FILEPATH = Path(TEST_STUDY_DIR, "geo", "gages.parquet")

PRIMARY_FILEPATH = Path("tests/data/retro/primary_obs.parquet")
SECONDARY_FILEPATH = Path("tests/data/retro/secondary_sim.parquet")
CROSSWALK_FILEPATH = Path("tests/data/retro/xwalk.parquet")


def test_ts_transfrom_sql():
    """Test metric queries for signature metrics."""

    # df = duckdb.query("""
    #     WITH initial_joined AS (
    #         SELECT
    #             sf.reference_time
    #             , sf.value_time as value_time
    #             , sf.location_id as secondary_location_id
    #             , pf.reference_time as primary_reference_time
    #             , sf.value as secondary_value
    #             , sf.configuration
    #             , sf.measurement_unit
    #             , sf.variable_name
    #             , pf.value as primary_value
    #             , pf.location_id as primary_location_id
    #             , sf.value_time - sf.reference_time as lead_time
    #             , abs(pf.value - sf.value) as absolute_difference
    #         FROM read_parquet('tests/data/test_study/timeseries/*_fcast.parquet') sf
    #         JOIN read_parquet('tests/data/test_study/geo/crosswalk.parquet') cf
    #             on cf.secondary_location_id = sf.location_id
    #         JOIN read_parquet("tests/data/test_study/timeseries/test_short_obs.parquet") pf
    #             on cf.primary_location_id = pf.location_id
    #             and sf.value_time = pf.value_time
    #             and sf.measurement_unit = pf.measurement_unit
    #             and sf.variable_name = pf.variable_name
    #         --no where clause
    #     ),
    #     joined AS (
    #         SELECT
    #             reference_time
    #             , value_time
    #             , secondary_location_id
    #             , secondary_value
    #             , configuration
    #             , measurement_unit
    #             , variable_name
    #             , primary_value
    #             , primary_location_id
    #             , lead_time
    #             , absolute_difference
    #         FROM
    #             initial_joined
    #     )
    #     , sigs AS (
    #         SELECT
    #             joined.primary_location_id
    #             , max(joined.primary_value) as monthly_primary_max_value
    #             , max(joined.secondary_value) as monthly_secondary_max_value
    #         FROM
    #             joined
    #         GROUP BY
    #             joined.primary_location_id
    #             AND date_part('month', joined.value_time)
    #     )
    #     , sig_metrics AS (
    #         SELECT
    #             sigs.primary_location_id
    #             , sum(monthly_secondary_max_value - monthly_primary_max_value) / sum(monthly_primary_max_value) AS monthly_peak_relative_bias
    #         FROM
    #             sigs
    #         GROUP BY
    #             sigs.primary_location_id
    #     )
    #     , metrics AS (
    #         SELECT
    #             joined.primary_location_id
    #             , sum(secondary_value - primary_value) / sum(primary_value) AS relative_bias
    #         FROM
    #             joined
    #         GROUP BY
    #             joined.primary_location_id
    #     )
    #     SELECT
    #         metrics.*
    #     FROM metrics
    # ;
    # """).to_df()
    # print(df)


    df = duckdb.query("""
        WITH initial_joined AS (
            SELECT
                sf.reference_time
                , sf.value_time as value_time
                , sf.location_id as secondary_location_id
                , pf.reference_time as primary_reference_time
                , sf.value as secondary_value
                , sf.configuration
                , sf.measurement_unit
                , sf.variable_name
                , pf.value as primary_value
                , pf.location_id as primary_location_id
                , sf.value_time - sf.reference_time as lead_time
                , abs(pf.value - sf.value) as absolute_difference
            FROM read_parquet('tests/data/retro/secondary_sim.parquet') sf
            JOIN read_parquet('tests/data/retro/xwalk.parquet') cf
                on cf.secondary_location_id = sf.location_id
            JOIN read_parquet('tests/data/retro/primary_obs.parquet') pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.measurement_unit = pf.measurement_unit
                and sf.variable_name = pf.variable_name
        ),
        joined AS (
            SELECT
                reference_time
                , value_time
                , secondary_location_id
                , secondary_value
                , configuration
                , measurement_unit
                , variable_name
                , primary_value
                , primary_location_id
                , lead_time
                , absolute_difference
            FROM
                initial_joined
        )
        , monthly AS (
            SELECT
                joined.primary_location_id
                , joined.configuration
                , date_trunc('month', joined.value_time) as month
                , max(joined.primary_value) as primary_max_value
                , max(joined.secondary_value) as secondary_max_value
            FROM
                joined
            GROUP BY
                joined.primary_location_id
                , joined.configuration
                , month
        )
        , monthly_metrics AS (
            SELECT
                monthly.primary_location_id
                , monthly.configuration
                , count(monthly.secondary_max_value) as monthly_secondary_max_value_count
                , sum(monthly.secondary_max_value - monthly.primary_max_value) / sum(monthly.primary_max_value) AS monthly_peak_relative_bias
            FROM
                monthly
            GROUP BY
                monthly.primary_location_id
                , monthly.configuration
        )
        , yearly AS (
            SELECT
                joined.primary_location_id
                , joined.configuration
                , date_part('year', joined.value_time) as year
                , max(joined.primary_value) as primary_max_value
                , max(joined.secondary_value) as secondary_max_value
            FROM
                joined
            GROUP BY
                joined.primary_location_id
                , joined.configuration
                , year
        )
        , yearly_metrics AS (
            SELECT
                yearly.primary_location_id
                , yearly.configuration
                , count(yearly.secondary_max_value) as yearly_secondary_max_value_count
                , sum(yearly.secondary_max_value - yearly.primary_max_value) / sum(yearly.primary_max_value) AS annual_peak_relative_bias
            FROM
                yearly
            GROUP BY
                yearly.primary_location_id
                , yearly.configuration
        )
        , metrics AS (
            SELECT
                joined.primary_location_id
                , joined.configuration
                , sum(secondary_value - primary_value) / sum(primary_value) AS relative_bias
            FROM
                joined
            GROUP BY
                joined.primary_location_id
                , joined.configuration
        )
        SELECT
            metrics.primary_location_id
            , metrics.configuration
            , monthly_secondary_max_value_count
            , annual_peak_relative_bias
        FROM metrics
        JOIN
            monthly_metrics
        ON metrics.primary_location_id = monthly_metrics.primary_location_id
        AND metrics.configuration = monthly_metrics.configuration
        JOIN
            yearly_metrics
        ON metrics.primary_location_id = yearly_metrics.primary_location_id
        AND metrics.configuration = yearly_metrics.configuration
    ;
    """).to_df()
    print(df)


def explore_observation_data():
    # ['usgs-09174600', 'usgs-12301933', 'usgs-06295900']
    obs = pd.read_parquet('tests/data/retro/primary_obs.parquet')
    obs_groups = obs.groupby(['location_id'])
    obs_g1 = obs_groups.get_group('usgs-09174600')
    obs_g1_yearly = obs_g1.groupby(obs_g1.value_time.dt.year).value.max()
    print(obs_g1_yearly)

    xw = pd.read_parquet('tests/data/retro/xwalk.parquet')
    # print(xw)

    sim = pd.read_parquet('tests/data/retro/secondary_sim.parquet')
    sim_groups = sim.groupby('location_id')
    sim_g1 = sim_groups.get_group('nwm21-18375940')
    sim_g1_yearly = sim_g1.groupby(sim_g1.value_time.dt.year).value.max()
    print(sim_g1_yearly)

    annual_peak_flow_bias = np.sum(sim_g1_yearly - obs_g1_yearly) / np.sum(obs_g1_yearly)
    print(annual_peak_flow_bias)


def test_annual_peak_flow_bias():
    df = tqp.get_metrics(
        primary_filepath=PRIMARY_FILEPATH,
        secondary_filepath=SECONDARY_FILEPATH,
        crosswalk_filepath=CROSSWALK_FILEPATH,
        include_metrics=['annual_peak_relative_bias'],
        group_by=['primary_location_id', 'configuration'],
        order_by=['primary_location_id', 'configuration'],
    )
    print(df)


if __name__ == "__main__":

    # test_ts_transfrom_sql()
    # explore_observation_data()
    # test_annual_peak_flow_bias()
    pass
