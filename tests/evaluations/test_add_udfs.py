"""Tests for the TEEHR UDFs."""
import pytest

import teehr
from teehr import RowLevelCalculatedFields as rcf
from teehr import TimeseriesAwareCalculatedFields as tcf

import pyspark.sql.types as T
import pyspark.sql.functions as F
import numpy as np
import baseflow
import pandas as pd
from datetime import timedelta


@pytest.mark.function_scope_evaluation_template
def test_add_row_udfs_null_reference(function_scope_evaluation_template):
    """Test adding row level UDFs with null reference time."""
    ev = function_scope_evaluation_template

    ev.joined_timeseries_view().add_calculated_fields([
        rcf.Month(),
        rcf.Year(),
        rcf.WaterYear(),
        rcf.Seasons()
    ]).write("joined_timeseries")

    nse = teehr.DeterministicMetrics.NashSutcliffeEfficiency()
    ev.table("joined_timeseries").aggregate(
        metrics=[nse],
        group_by=["primary_location_id"]
    ).write(table_name="metrics", write_mode="create_or_replace")


@pytest.mark.session_scope_test_warehouse
def test_add_row_udfs(session_scope_test_warehouse):
    """Test adding row level UDFs."""
    ev = session_scope_test_warehouse

    # Read table in fixture.
    sdf = ev.table("joined_timeseries").to_sdf()

    sdf = rcf.Month().apply_to(sdf)
    _ = sdf.toPandas()

    sdf = rcf.Year().apply_to(sdf)
    _ = sdf.toPandas()

    sdf = rcf.WaterYear().apply_to(sdf)
    _ = sdf.toPandas()

    sdf = rcf.NormalizedFlow().apply_to(sdf)
    _ = sdf.toPandas()

    sdf = rcf.Seasons().apply_to(sdf)
    _ = sdf.toPandas()

    sdf = rcf.ForecastLeadTime().apply_to(sdf)
    _ = sdf.toPandas()

    sdf = rcf.ForecastLeadTimeBins().apply_to(sdf)
    _ = sdf.toPandas()

    sdf = rcf.ThresholdValueExceeded(
            threshold_field_name="year_2_discharge"
        ).apply_to(sdf)
    df1 = sdf.toPandas()

    sdf = rcf.ThresholdValueNotExceeded(
            threshold_field_name="year_2_discharge"
        ).apply_to(sdf)
    df2 = sdf.toPandas()
    assert all(
        df1['threshold_value_exceeded'] == ~df2['threshold_value_not_exceeded']
    )

    sdf = rcf.DayOfYear().apply_to(sdf)
    _ = sdf.toPandas()

    cols = sdf.columns
    check_sdf = sdf[sdf["primary_location_id"] == "gage-A"]
    check_sdf = check_sdf.orderBy("value_time")

    assert "month" in cols
    assert sdf.schema["month"].dataType == T.IntegerType()
    check_vals = check_sdf.select("month").distinct().collect()
    for row in check_vals:
        assert row["month"] == 1

    assert "year" in cols
    assert sdf.schema["year"].dataType == T.IntegerType()
    check_vals = check_sdf.select("year").distinct().collect()
    for row in check_vals:
        assert row["year"] == 2022

    assert "water_year" in cols
    assert sdf.schema["water_year"].dataType == T.IntegerType()
    check_vals = check_sdf.select("water_year").distinct().collect()
    for row in check_vals:
        assert row["water_year"] == 2022

    assert "normalized_flow" in cols
    assert sdf.schema["normalized_flow"].dataType == T.FloatType()
    check_vals = check_sdf.select("normalized_flow").collect()
    # assert np.round(check_vals[0]["normalized_flow"], 3) == 0.003  # TODO: Why? -- need to order by value_time
    assert np.round(check_vals[0]["normalized_flow"], 3) == 0.001

    assert "season" in cols
    assert sdf.schema["season"].dataType == T.StringType()
    check_vals = check_sdf.select("season").distinct().collect()
    for row in check_vals:
        assert row["season"] in ["winter", "spring", "summer", "fall"]

    assert "forecast_lead_time" in cols
    assert sdf.schema["forecast_lead_time"].dataType == T.DayTimeIntervalType()
    row = check_sdf.collect()[1]
    expected_val = (row["value_time"] - row["reference_time"]).total_seconds()
    test_val = row["forecast_lead_time"].total_seconds()
    assert expected_val == test_val

    assert "threshold_value_exceeded" in cols
    assert sdf.schema["threshold_value_exceeded"].dataType == T.BooleanType()
    check_vals = check_sdf.select(
        "threshold_value_exceeded").distinct().collect()
    assert check_vals[0]["threshold_value_exceeded"] is False

    assert "day_of_year" in cols
    assert sdf.schema["day_of_year"].dataType == T.IntegerType()
    check_vals = check_sdf.select("day_of_year").distinct().collect()
    for row in check_vals:
        assert row["day_of_year"] in [1, 2]


@pytest.mark.function_scope_small_ensemble_warehouse
def test_forecast_lead_time_bins(function_scope_small_ensemble_warehouse):
    """Test ForecastLeadTimeBins UDF."""
    ev = function_scope_small_ensemble_warehouse

    # test with single bin size
    fcst_bins_static = teehr.RowLevelCalculatedFields.ForecastLeadTimeBins(
        bin_size=pd.Timedelta(hours=6)
    )
    sdf = ev.table("joined_timeseries").add_calculated_fields([
        fcst_bins_static,
    ]).to_sdf()

    sorted_sdf = sdf.orderBy(
        "primary_location_id",
        "configuration_name",
        "member",
        "reference_time",
        "value_time"
    )

    assert sorted_sdf.select('forecast_lead_time_bin').distinct().count() == 9

    # try with dynamic bin sizes that DO encompass full lead time range
    bin = [
        {'start_inclusive': pd.Timedelta(hours=0),
         'end_exclusive': pd.Timedelta(hours=6)},
        {'start_inclusive': pd.Timedelta(hours=6),
         'end_exclusive': pd.Timedelta(hours=12)},
        {'start_inclusive': pd.Timedelta(hours=12),
         'end_exclusive': pd.Timedelta(hours=18)},
        {'start_inclusive': pd.Timedelta(hours=18),
         'end_exclusive': pd.Timedelta(days=1)},
        {'start_inclusive': pd.Timedelta(days=1),
         'end_exclusive': pd.Timedelta(days=1, hours=12)},
        {'start_inclusive': pd.Timedelta(days=1, hours=12),
         'end_exclusive': pd.Timedelta(days=2)},
        {'start_inclusive': pd.Timedelta(days=2),
         'end_exclusive': pd.Timedelta(days=3)},
    ]
    fcst_bins_dynamic = teehr.RowLevelCalculatedFields.ForecastLeadTimeBins(
        bin_size=bin,
    )

    sdf = ev.table("joined_timeseries").add_calculated_fields([
        fcst_bins_dynamic,
    ]).to_sdf()
    sorted_sdf = sdf.orderBy(
        "primary_location_id",
        "configuration_name",
        "member",
        "reference_time",
        "value_time"
        )
    assert sorted_sdf.select('forecast_lead_time_bin').distinct().count() == 7

    # try with dynamic bin sizes that DO NOT encompass full lead time range
    bin = [
        {'start_inclusive': pd.Timedelta(hours=0),
         'end_exclusive': pd.Timedelta(hours=6)},
        {'start_inclusive': pd.Timedelta(hours=6),
         'end_exclusive': pd.Timedelta(hours=12)},
        {'start_inclusive': pd.Timedelta(hours=12),
         'end_exclusive': pd.Timedelta(hours=18)},
        {'start_inclusive': pd.Timedelta(hours=18),
         'end_exclusive': pd.Timedelta(days=1)},
        {'start_inclusive': pd.Timedelta(days=1),
         'end_exclusive': pd.Timedelta(days=1, hours=12)},
    ]
    fcst_bins_dynamic = teehr.RowLevelCalculatedFields.ForecastLeadTimeBins(
        bin_size=bin,
    )
    sdf = ev.table("joined_timeseries").add_calculated_fields([
        fcst_bins_dynamic,
    ]).to_sdf()
    sorted_sdf = sdf.orderBy(
        "primary_location_id",
        "configuration_name",
        "member",
        "reference_time",
        "value_time"
        )
    assert sorted_sdf.select('forecast_lead_time_bin').distinct().count() == 6
    assert 'P1DT12H_P2DT0H' in [row['forecast_lead_time_bin'] for row in
                                sorted_sdf.select(
                                     'forecast_lead_time_bin'
                                     ).distinct().collect()]

    # try with dynamic bin sizes w/ string dict keys that DO encompass full
    # lead time range
    bin = {
        'bin_1': {'start_inclusive': pd.Timedelta(hours=0),
                  'end_exclusive': pd.Timedelta(hours=6)},
        'bin_2': {'start_inclusive': pd.Timedelta(hours=6),
                  'end_exclusive': pd.Timedelta(hours=12)},
        'bin_3': {'start_inclusive': pd.Timedelta(hours=12),
                  'end_exclusive': pd.Timedelta(hours=18)},
        'bin_4': {'start_inclusive': pd.Timedelta(hours=18),
                  'end_exclusive': pd.Timedelta(days=1)},
        'bin_5': {'start_inclusive': pd.Timedelta(days=1),
                  'end_exclusive': pd.Timedelta(days=1, hours=12)},
        'bin_6': {'start_inclusive': pd.Timedelta(days=1, hours=12),
                  'end_exclusive': pd.Timedelta(days=2)},
        'bin_7': {'start_inclusive': pd.Timedelta(days=2),
                  'end_exclusive': pd.Timedelta(days=3)},
    }
    fcst_bins_dynamic = teehr.RowLevelCalculatedFields.ForecastLeadTimeBins(
        bin_size=bin
    )
    sdf = ev.table("joined_timeseries").add_calculated_fields([
        fcst_bins_dynamic,
    ]).to_sdf()
    sorted_sdf = sdf.orderBy(
        "primary_location_id",
        "configuration_name",
        "member",
        "reference_time",
        "value_time"
        )
    assert sorted_sdf.select('forecast_lead_time_bin').distinct().count() == 7

    # try with dynamic bin sizes w/ string dict keys that DO NOT encompass
    # full lead time range
    bin = {
        'bin_1': {'start_inclusive': pd.Timedelta(hours=0),
                  'end_exclusive': pd.Timedelta(hours=6)},
        'bin_2': {'start_inclusive': pd.Timedelta(hours=6),
                  'end_exclusive': pd.Timedelta(hours=12)},
        'bin_3': {'start_inclusive': pd.Timedelta(hours=12),
                  'end_exclusive': pd.Timedelta(hours=18)},
        'bin_4': {'start_inclusive': pd.Timedelta(hours=18),
                  'end_exclusive': pd.Timedelta(days=1)},
        'bin_5': {'start_inclusive': pd.Timedelta(days=1),
                  'end_exclusive': pd.Timedelta(days=1, hours=12)},
        'bin_6': {'start_inclusive': pd.Timedelta(days=1, hours=12),
                  'end_exclusive': pd.Timedelta(days=2)},
    }
    fcst_bins_dynamic = teehr.RowLevelCalculatedFields.ForecastLeadTimeBins(
        bin_size=bin
    )
    sdf = ev.table("joined_timeseries").add_calculated_fields([
        fcst_bins_dynamic,
    ]).to_sdf()
    sorted_sdf = sdf.orderBy(
        "primary_location_id",
        "configuration_name",
        "member",
        "reference_time",
        "value_time"
        )
    assert sorted_sdf.select('forecast_lead_time_bin').distinct().count() == 7
    assert 'overflow' in [row['forecast_lead_time_bin'] for row in
                          sorted_sdf.select(
                              'forecast_lead_time_bin'
                              ).distinct().collect()]

    # try mixed type dynamic bin sizes w/ string dict keys that DO encompass
    # the full lead time range
    bin = {
        'bin_1': {'start_inclusive': '0 hours',
                  'end_exclusive': '6 hours'},
        'bin_2': {'start_inclusive': pd.Timedelta('6 hours'),
                  'end_exclusive': pd.Timedelta(hours=12)},
        'bin_3': {'start_inclusive': timedelta(hours=12),
                  'end_exclusive': timedelta(hours=18)},
        'bin_4': {'start_inclusive': '18 hours',
                  'end_exclusive': pd.Timedelta('1 days')},
        'bin_5': {'start_inclusive': '1 days',
                  'end_exclusive': timedelta(days=1, hours=12)},
        'bin_6': {'start_inclusive': pd.Timedelta(days=1, hours=12),
                  'end_exclusive': '2 days'},
        'bin_7': {'start_inclusive': timedelta(days=2),
                  'end_exclusive': '3 days'},
    }
    fcst_bins_dynamic = teehr.RowLevelCalculatedFields.ForecastLeadTimeBins(
        bin_size=bin
    )
    sdf = ev.table("joined_timeseries").add_calculated_fields([
        fcst_bins_dynamic,
    ]).to_sdf()
    sorted_sdf = sdf.orderBy(
        "primary_location_id",
        "configuration_name",
        "member",
        "reference_time",
        "value_time"
        )
    assert sorted_sdf.select('forecast_lead_time_bin').distinct().count() == 7


@pytest.mark.function_scope_two_location_warehouse
def test_baseflow_methods(function_scope_two_location_warehouse):
    """Test baseflow separation algorithms against reference implementation."""
    ev = function_scope_two_location_warehouse

    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()

    # set up input to baseflow package for native testing
    pdf = sdf.toPandas()
    pdf = pdf.sort_values(by='value_time')
    streamflow = pd.Series(pdf['primary_value'].values,
                           index=pd.to_datetime(pdf['value_time']))

    # test Lyne-Hollick baseflow
    lhbf = tcf.LyneHollickBaseflow()
    sdf = lhbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='LH', return_kge=False)
    control = result[0]['LH'].values.sum()
    test = sdf.select('lyne_hollick_baseflow').toPandas()[
        'lyne_hollick_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Chapman baseflow
    chapbf = tcf.ChapmanBaseflow()
    sdf = chapbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='Chapman', return_kge=False)
    control = result[0]['Chapman'].values.sum()
    test = sdf.select('chapman_baseflow').toPandas()['chapman_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Chapman-Maxwell baseflow
    cmbf = tcf.ChapmanMaxwellBaseflow()
    sdf = cmbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='CM', return_kge=False)
    control = result[0]['CM'].values.sum()
    test = sdf.select('chapman_maxwell_baseflow').toPandas()[
        'chapman_maxwell_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Boughton baseflow
    bbf = tcf.BoughtonBaseflow()
    sdf = bbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='Boughton', return_kge=False)
    control = result[0]['Boughton'].values.sum()
    test = sdf.select('boughton_baseflow').toPandas()['boughton_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Furey baseflow
    fbf = tcf.FureyBaseflow()
    sdf = fbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='Furey', return_kge=False)
    control = result[0]['Furey'].values.sum()
    test = sdf.select('furey_baseflow').toPandas()['furey_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Eckhardt baseflow
    eckbf = tcf.EckhardtBaseflow()
    sdf = eckbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='Eckhardt', return_kge=False)
    control = result[0]['Eckhardt'].values.sum()
    test = sdf.select('eckhardt_baseflow').toPandas()['eckhardt_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test EWMA baseflow
    ewmabf = tcf.EWMABaseflow()
    sdf = ewmabf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='EWMA', return_kge=False)
    control = result[0]['EWMA'].values.sum()
    test = sdf.select('ewma_baseflow').toPandas()['ewma_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Willems baseflow
    wbf = tcf.WillemsBaseflow()
    sdf = wbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='Willems', return_kge=False)
    control = result[0]['Willems'].values.sum()
    test = sdf.select('willems_baseflow').toPandas()['willems_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test UKIH baseflow
    ukihbf = tcf.UKIHBaseflow()
    sdf = ukihbf.apply_to(sdf)
    result = baseflow.single(series=streamflow, method='UKIH', return_kge=False)
    control = result[0]['UKIH'].values.sum()
    test = sdf.select('ukih_baseflow').toPandas()['ukih_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)


@pytest.mark.function_scope_two_location_warehouse
def test_baseflow_period_detection(function_scope_two_location_warehouse):
    """Test baseflow period detection with and without event threshold."""
    ev = function_scope_two_location_warehouse

    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()

    # compute lyne-hollick baseflow first (required input)
    sdf = tcf.LyneHollickBaseflow().apply_to(sdf)

    # no event_threshold
    bfdp = tcf.BaseflowPeriodDetection(baseflow_field_name='lyne_hollick_baseflow')
    sdf = bfdp.apply_to(sdf)
    event_count = sdf.select('baseflow_period_id').distinct().count()
    assert event_count == 130

    # with event_threshold
    bfdp = tcf.BaseflowPeriodDetection(
        baseflow_field_name='lyne_hollick_baseflow',
        event_threshold=1.5,
        output_baseflow_period_field_name='baseflow_period_2',
        output_baseflow_period_id_field_name='baseflow_period_id_2'
    )
    sdf = bfdp.apply_to(sdf)
    event_count = sdf.select('baseflow_period_id_2').distinct().count()
    assert event_count == 208


@pytest.mark.function_scope_two_location_warehouse
def test_percentile_event_detection(function_scope_two_location_warehouse):
    """Test above/below percentile event detection variants."""
    ev = function_scope_two_location_warehouse

    # above percentile with event ids (default)
    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    ped = tcf.AbovePercentileEventDetection()
    sdf = ped.apply_to(sdf)
    assert sdf.filter(F.col('event_above_id').isNull()).count() == 0
    assert sdf.filter(~F.col('event_above')).count() > 0
    assert sdf.filter(
        (~F.col('event_above')) & (F.col('event_above_id').isNull())
    ).count() == 0

    # above percentile skip_event_id
    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    ped = tcf.AbovePercentileEventDetection(skip_event_id=True)
    sdf = ped.apply_to(sdf)
    assert sdf.filter(sdf.event_above).count() == 14823

    # above percentile add_quantile_field
    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    ped = tcf.AbovePercentileEventDetection(add_quantile_field=True)
    sdf = ped.apply_to(sdf)
    quantile = sdf.select("quantile_value").distinct().collect()[0][0]
    assert np.isclose(quantile, 37.66, atol=0.01)

    # below percentile with event ids
    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    ped = tcf.BelowPercentileEventDetection()
    sdf = ped.apply_to(sdf)
    assert sdf.filter(F.col('event_below_id').isNull()).count() == 0
    assert sdf.filter(~F.col('event_below')).count() > 0
    assert sdf.filter(
        (~F.col('event_below')) & (F.col('event_below_id').isNull())
    ).count() == 0


@pytest.mark.function_scope_two_location_warehouse
def test_percentile_event_detection_grouped_spark(function_scope_two_location_warehouse):
    """Test grouped percentile event detection using spark-native batching."""
    ev = function_scope_two_location_warehouse

    cf1 = tcf.AbovePercentileEventDetection(
        quantile=0.85,
        output_event_field_name="event_above_q85",
        output_event_id_field_name="event_above_q85_id",
        skip_event_id=True,
    )
    cf2 = tcf.AbovePercentileEventDetection(
        quantile=0.90,
        output_event_field_name="event_above_q90",
        output_event_id_field_name="event_above_q90_id",
        skip_event_id=True,
    )

    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).add_calculated_fields([cf1, cf2], engine="spark").to_sdf()

    assert "event_above_q85" in sdf.columns
    assert "event_above_q90" in sdf.columns

    q85_count = sdf.filter(F.col("event_above_q85")).count()
    q90_count = sdf.filter(F.col("event_above_q90")).count()
    assert q85_count >= q90_count

    # Both quantiles should be computed in one grouped percentile pass.
    plan_text = sdf._jdf.queryExecution().optimizedPlan().toString().lower()
    assert plan_text.count("percentile_approx") == 1


@pytest.mark.function_scope_two_location_warehouse
def test_percentile_event_detection_grouped_spark_different_groups(
    function_scope_two_location_warehouse,
):
    """Percentile detectors with different group keys must not be batched."""
    ev = function_scope_two_location_warehouse

    # Same value/time fields, different uniqueness_fields/grouping.
    cf1 = tcf.AbovePercentileEventDetection(
        quantile=0.85,
        uniqueness_fields=["primary_location_id", "configuration_name"],
        output_event_field_name="event_above_cfg",
        output_event_id_field_name="event_above_cfg_id",
        skip_event_id=True,
    )
    cf2 = tcf.AbovePercentileEventDetection(
        quantile=0.85,
        uniqueness_fields=["primary_location_id"],
        output_event_field_name="event_above_loc",
        output_event_id_field_name="event_above_loc_id",
        skip_event_id=True,
    )

    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).add_calculated_fields([cf1, cf2], engine="spark").to_sdf()

    assert "event_above_cfg" in sdf.columns
    assert "event_above_loc" in sdf.columns

    # Different grouping keys should force separate percentile computations.
    plan_text = sdf._jdf.queryExecution().optimizedPlan().toString().lower()
    assert plan_text.count("percentile_approx") == 2


@pytest.mark.function_scope_two_location_warehouse
def test_threshold_event_detection(function_scope_two_location_warehouse):
    """Test above/below threshold event detection variants."""
    ev = function_scope_two_location_warehouse

    # above threshold, string value cast to float, skip event ids
    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    sdf = sdf.withColumn("threshold", F.lit("50.0"))
    ted = tcf.AboveThresholdEventDetection(
        threshold_field_name="threshold",
        skip_event_id=True
    )
    sdf = ted.apply_to(sdf)
    assert "event_above" in sdf.columns
    assert sdf.filter(sdf.event_above).count() > 0

    # above threshold with event ids
    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    sdf = sdf.withColumn("threshold", F.lit("50.0"))
    ted = tcf.AboveThresholdEventDetection(threshold_field_name="threshold")
    sdf = ted.apply_to(sdf)
    assert "event_above_id" in sdf.columns
    assert sdf.select('event_above_id').distinct().count() > 0
    assert sdf.filter(F.col('event_above_id').isNull()).count() == 0
    assert sdf.filter(~F.col('event_above')).count() > 0
    assert sdf.filter(
        (~F.col('event_above')) & (F.col('event_above_id').isNull())
    ).count() == 0

    # below threshold with event ids
    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    sdf = sdf.withColumn("threshold", F.lit("50.0"))
    ted = tcf.BelowThresholdEventDetection(threshold_field_name="threshold")
    sdf = ted.apply_to(sdf)
    assert "event_below_id" in sdf.columns
    assert sdf.select('event_below_id').distinct().count() > 0
    assert sdf.filter(F.col('event_below_id').isNull()).count() == 0
    assert sdf.filter(~F.col('event_below')).count() > 0
    assert sdf.filter(
        (~F.col('event_below')) & (F.col('event_below_id').isNull())
    ).count() == 0


@pytest.mark.function_scope_two_location_warehouse
def test_calculated_fields_auto_mixed_spark_and_python(function_scope_two_location_warehouse):
    """Test mixed spark-native and python calculated fields in auto mode."""
    ev = function_scope_two_location_warehouse

    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    sdf = sdf.withColumn("threshold", F.lit("50.0"))

    ted = tcf.AboveThresholdEventDetection(
        threshold_field_name="threshold",
        skip_event_id=True,
    )
    seasons = rcf.Seasons()

    # Use a temporary accessor chain so add_calculated_fields engine routing is exercised.
    ev._write.to_warehouse(
        source_data=sdf,
        table_name="joined_timeseries",
        write_mode="create_or_replace",
    )

    result = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).add_calculated_fields([ted, seasons], engine="auto").to_sdf()

    assert "event_above" in result.columns
    assert "season" in result.columns
    assert result.filter(F.col("event_above")).count() > 0


@pytest.mark.function_scope_two_location_warehouse
def test_calculated_fields_auto_row_level_default_stays_spark(
    function_scope_two_location_warehouse,
):
    """Row-level fields should stay Spark-native unless python is explicitly requested."""
    ev = function_scope_two_location_warehouse

    result = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).add_calculated_fields([
        rcf.Month(output_field_name="month_default"),
        rcf.Seasons(output_field_name="season_default"),
    ], engine="auto").to_sdf()

    assert "month_default" in result.columns
    assert "season_default" in result.columns

    physical_plan = result._jdf.queryExecution().executedPlan().toString().lower()
    assert "arrowevalpython" not in physical_plan


@pytest.mark.function_scope_two_location_warehouse
def test_calculated_fields_python_row_level_explicit_engine(function_scope_two_location_warehouse):
    """Row-level python backend should run when engine='python' is explicitly requested."""
    ev = function_scope_two_location_warehouse

    spark_result = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).add_calculated_fields([
        rcf.Month(output_field_name="month_spark"),
        rcf.Seasons(output_field_name="season_spark"),
    ], engine="spark").to_sdf()

    python_result = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).add_calculated_fields([
        rcf.Month(output_field_name="month_python"),
        rcf.Seasons(output_field_name="season_python"),
    ], engine="python").to_sdf()

    result = spark_result.join(
        python_result.select("value_time", "month_python", "season_python"),
        on=["value_time"],
        how="inner",
    )

    mismatched_month = result.filter(F.col("month_spark") != F.col("month_python")).count()
    mismatched_season = result.filter(F.col("season_spark") != F.col("season_python")).count()

    assert mismatched_month == 0
    assert mismatched_season == 0

    physical_plan = python_result._jdf.queryExecution().executedPlan().toString().lower()
    assert "arrowevalpython" in physical_plan


@pytest.mark.function_scope_two_location_warehouse
def test_calculated_fields_spark_engine_accepts_row_level(function_scope_two_location_warehouse):
    """Spark engine should still accept row-level fields."""
    ev = function_scope_two_location_warehouse

    result = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).add_calculated_fields([
        rcf.Month(output_field_name="month_spark_only")
    ], engine="spark").to_sdf()
    assert "month_spark_only" in result.columns


@pytest.mark.function_scope_two_location_warehouse
def test_exceedance_probability(function_scope_two_location_warehouse):
    """Test exceedance probability UDF."""
    ev = function_scope_two_location_warehouse

    sdf = ev.table("joined_timeseries").filter(
        "primary_location_id = 'usgs-14316700'"
    ).to_sdf()
    ep = tcf.ExceedanceProbability()
    sdf = ep.apply_to(sdf)
    assert "exceedance_probability" in sdf.columns
    min_ep = sdf.select(F.min("exceedance_probability")).collect()[0][0]
    max_ep = sdf.select(F.max("exceedance_probability")).collect()[0][0]
    assert np.isclose(min_ep, 0.0, atol=0.001)
    assert np.isclose(max_ep, 1.0, atol=0.001)


@pytest.mark.function_scope_evaluation_template
def test_add_udfs_write(function_scope_evaluation_template):
    """Test adding UDFs and write DataFrame back to table."""
    ev = function_scope_evaluation_template

    # First join with event detection
    ped = tcf.AbovePercentileEventDetection()
    ev.joined_timeseries_view().add_calculated_fields(ped).write("joined_timeseries")

    # Add forecast lead time to the persisted table (new instance loads from table)
    flt = rcf.ForecastLeadTime()
    ev.table("joined_timeseries").add_calculated_fields(flt).write("joined_timeseries")

    new_sdf = ev.table("joined_timeseries").to_sdf()
    cols = new_sdf.columns
    assert "event_above" in cols
    assert "event_above_id" in cols
    assert "forecast_lead_time" in cols


@pytest.mark.function_scope_test_warehouse
def test_location_event_detection(function_scope_test_warehouse):
    """Test event detection and metrics per event."""
    ev = function_scope_test_warehouse

    ped = tcf.AbovePercentileEventDetection()
    sdf = ev.table("joined_timeseries").add_calculated_fields(ped).filter(
        "event_above"
    ).aggregate(
        group_by=["configuration_name",
                  "primary_location_id",
                  "event_above_id"],
        metrics=[
            teehr.Signatures.Maximum(
                input_field_names=["primary_value"],
                output_field_name="max_primary_value"
            ),
            teehr.Signatures.Maximum(
                input_field_names=["secondary_value"],
                output_field_name="max_secondary_value"
            )
        ]
    ).to_sdf()

    assert sdf.count() > 0
    assert sdf.filter(F.col("event_above_id").isNull()).count() == 0

    assert "configuration_name" in sdf.columns
    assert "primary_location_id" in sdf.columns
    assert "event_above_id" in sdf.columns
    assert "max_primary_value" in sdf.columns
    assert "max_secondary_value" in sdf.columns
