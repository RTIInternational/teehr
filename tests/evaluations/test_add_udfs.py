"""Tests for the TEEHR UDFs."""
import tempfile
import teehr
from teehr import RowLevelCalculatedFields as rcf
from teehr import TimeseriesAwareCalculatedFields as tcf

import pyspark.sql.types as T
import numpy as np
import baseflow
import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from data.setup_v0_3_study import setup_v0_3_study  # noqa


def test_add_row_udfs_null_reference(tmp_path):
    """Test adding row level UDFs with null reference time."""
    ev = teehr.Evaluation(
        local_warehouse_dir=tmp_path,
        create_local_dir=True,
        check_evaluation_version=False,
    )
    ev.clone_from_s3(remote_namespace_name="e0_2_location_example")

    ev.joined_timeseries.create(add_attrs=False, execute_scripts=False)

    ev.joined_timeseries.add_calculated_fields([
        rcf.Month(),
        rcf.Year(),
        rcf.WaterYear(),
        rcf.Seasons()
    ]).write()

    nse = teehr.DeterministicMetrics.NashSutcliffeEfficiency()
    ev.metrics.query(
        include_metrics=[nse],
        group_by=["primary_location_id"]
    ).write_to_warehouse(table_name="metrics", write_mode="create_or_replace")

    ev.spark.stop()


def test_add_row_udfs(tmp_path):
    """Test adding row level UDFs."""
    ev = setup_v0_3_study(tmp_path)
    sdf = ev.joined_timeseries.to_sdf()

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
    assert np.round(check_vals[0]["normalized_flow"], 3) == 0.003

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

    ev.spark.stop()


def test_add_timeseries_udfs(tmp_path):
    """Test adding a timeseries aware UDF."""
    # utilize e0_2_location_example from s3 to satisfy baseflow POR reqs
    ev = teehr.Evaluation(tmp_path, create_local_dir=True)
    ev.clone_from_s3(evaluation_name="e0_2_location_example",
                     primary_location_ids=["usgs-14316700"])
    sdf = ev.joined_timeseries.to_sdf()

    # set up input to baseflow package for native testing
    pdf = sdf.toPandas()
    pdf = pdf.sort_values(by='value_time')
    streamflow = pd.Series(pdf['primary_value'].values,
                           index=pd.to_datetime(pdf['value_time']))

    # test Lyne-Hollick baseflow
    lhbf = tcf.LyneHollickBaseflow()
    sdf = lhbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='LH',
                             return_kge=False)
    df = result[0]
    control = df['LH'].values.sum()
    test = sdf.select('lyne_hollick_baseflow').toPandas()[
        'lyne_hollick_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Chapman baseflow
    chapbf = tcf.ChapmanBaseflow()
    sdf = chapbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='Chapman',
                             return_kge=False)
    df = result[0]
    control = df['Chapman'].values.sum()
    test = sdf.select('chapman_baseflow').toPandas()[
        'chapman_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Chapman-Maxwell baseflow
    cmbf = tcf.ChapmanMaxwellBaseflow()
    sdf = cmbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='CM',
                             return_kge=False)
    df = result[0]
    control = df['CM'].values.sum()
    test = sdf.select('chapman_maxwell_baseflow').toPandas()[
        'chapman_maxwell_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Boughton baseflow
    bbf = tcf.BoughtonBaseflow()
    sdf = bbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='Boughton',
                             return_kge=False)
    df = result[0]
    control = df['Boughton'].values.sum()
    test = sdf.select('boughton_baseflow').toPandas()[
        'boughton_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Furey baseflow
    fbf = tcf.FureyBaseflow()
    sdf = fbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='Furey',
                             return_kge=False)
    df = result[0]
    control = df['Furey'].values.sum()
    test = sdf.select('furey_baseflow').toPandas()[
        'furey_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Eckhardt baseflow
    eckbf = tcf.EckhardtBaseflow()
    sdf = eckbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='Eckhardt',
                             return_kge=False)
    df = result[0]
    control = df['Eckhardt'].values.sum()
    test = sdf.select('eckhardt_baseflow').toPandas()[
        'eckhardt_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test EWMA baseflow
    ewmabf = tcf.EWMABaseflow()
    sdf = ewmabf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='EWMA',
                             return_kge=False)
    df = result[0]
    control = df['EWMA'].values.sum()
    test = sdf.select('ewma_baseflow').toPandas()['ewma_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test Willems baseflow
    wbf = tcf.WillemsBaseflow()
    sdf = wbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='Willems',
                             return_kge=False)
    df = result[0]
    control = df['Willems'].values.sum()
    test = sdf.select('willems_baseflow').toPandas()[
        'willems_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test UKIH baseflow
    ukihbf = tcf.UKIHBaseflow()
    sdf = ukihbf.apply_to(sdf)
    result = baseflow.single(series=streamflow,
                             method='UKIH',
                             return_kge=False)
    df = result[0]
    control = df['UKIH'].values.sum()
    test = sdf.select('ukih_baseflow').toPandas()['ukih_baseflow'].values.sum()
    assert np.isclose(control, test, atol=0.001)

    # test baseflow period detection (no event_threshold)
    bfdp = tcf.BaseflowPeriodDetection(
        baseflow_field_name='lyne_hollick_baseflow'
        )
    sdf = bfdp.apply_to(sdf)
    event_count = sdf.select('baseflow_period_id').distinct().count()
    assert event_count == 130

    # test baseflow period detection (w/ event_threshold)
    bfdp = tcf.BaseflowPeriodDetection(
        baseflow_field_name='lyne_hollick_baseflow',
        event_threshold=1.5,
        output_baseflow_period_field_name='baseflow_period_2',
        output_baseflow_period_id_field_name='baseflow_period_id_2'
    )
    sdf = bfdp.apply_to(sdf)
    event_count = sdf.select('baseflow_period_id_2').distinct().count()
    assert event_count == 208

    # test percentile event detection (default)
    ped = tcf.PercentileEventDetection()
    sdf = ped.apply_to(sdf)
    event_count = sdf.select('event_id').distinct().count()
    assert event_count == 219

    # test percentile event detection (no event-id)
    sdf = ev.joined_timeseries.to_sdf()
    ped = tcf.PercentileEventDetection(
        skip_event_id=True
    )
    sdf = ped.apply_to(sdf)
    num_event_timesteps = sdf.filter(sdf.event == True).count()
    assert num_event_timesteps == 14823

    # test percentile event detection (return quantile value)
    sdf = ev.joined_timeseries.to_sdf()
    ped = tcf.PercentileEventDetection(
        add_quantile_field=True
    )
    sdf = ped.apply_to(sdf)
    distinct_quantiles = sdf.select("quantile_value").distinct().collect()
    quantile = distinct_quantiles[0][0]
    assert np.isclose(quantile, 37.66, atol=0.01)

    ev.spark.stop()


def test_add_udfs_write(tmp_path):
    """Test adding UDFs and write DataFrame back to table."""
    ev = setup_v0_3_study(tmp_path)

    ped = tcf.PercentileEventDetection()
    ev.joined_timeseries.add_calculated_fields(ped).write()

    # flt = rcf.ForecastLeadTime()
    # ev.joined_timeseries.add_calculated_fields(flt).write()

    new_sdf = ev.joined_timeseries.to_sdf()

    cols = new_sdf.columns
    assert "event" in cols
    assert "event_id" in cols
    # assert "forecast_lead_time" in cols

    ev.spark.stop()


def test_location_event_detection(tmp_path):
    """Test event detection and metrics per event."""
    ev = setup_v0_3_study(tmp_path)

    ped = tcf.PercentileEventDetection()
    sdf = ev.metrics.add_calculated_fields(ped).query(
        group_by=["configuration_name", "primary_location_id", "event_id"],
        include_metrics=[
            teehr.SignatureMetrics.Maximum(
                input_field_names=["primary_value"],
                output_field_name="max_primary_value"
            ),
            teehr.SignatureMetrics.Maximum(
                input_field_names=["secondary_value"],
                output_field_name="max_secondary_value"
            )
        ]
    ).to_sdf()

    assert sdf.count() == 6

    assert "configuration_name" in sdf.columns
    assert "primary_location_id" in sdf.columns
    assert "event_id" in sdf.columns
    assert "max_primary_value" in sdf.columns
    assert "max_secondary_value" in sdf.columns

    ev.spark.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(
        prefix="teehr-"
    ) as tempdir:
        test_add_row_udfs_null_reference(
            tempfile.mkdtemp(
                prefix="0-",
                dir=tempdir
            )
        )
        test_add_row_udfs(
            tempfile.mkdtemp(
                prefix="1-",
                dir=tempdir
            )
        )
        test_add_timeseries_udfs(
            tempfile.mkdtemp(
                prefix="2-",
                dir=tempdir
            )
        )
        test_add_udfs_write(
            tempfile.mkdtemp(
                prefix="3-",
                dir=tempdir
            )
        )
        test_location_event_detection(
            tempfile.mkdtemp(
                prefix="5-",
                dir=tempdir
            )
        )
