"""Tests for reading data from S3 using TEEHR."""
import teehr


def test_read_from_s3():
    """Test reading data from S3 using TEEHR."""
    # path = "s3a://ciroh-rti-public-data/teehr-data-warehouse/v0_4_evaluations/e1_camels_daily_streamflow"
    path = "s3a://ciroh-rti-public-data/teehr-data-warehouse/v0_4_evaluations/e0_2_location_example"

    ev = teehr.Evaluation(dir_path=path, create_dir=True)

    unit_cnt = ev.units.to_sdf().count()
    assert unit_cnt == 4

    var_cnt = ev.variables.to_sdf().count()
    assert var_cnt == 3

    conf_cnt = ev.configurations.to_sdf().count()
    assert conf_cnt == 2

    attrs_cnt = ev.attributes.to_sdf().count()
    assert attrs_cnt == 26

    loc_cnt = ev.locations.to_sdf().count()
    assert loc_cnt == 2

    loc_attr_cnt = ev.location_attributes.to_sdf().count()
    assert loc_attr_cnt == 50

    loc_xwalk_cnt = ev.location_crosswalks.to_sdf().count()
    assert loc_xwalk_cnt == 2

    pt_cnt = ev.primary_timeseries.to_sdf().count()
    assert pt_cnt == 200_350

    st_cnt = ev.secondary_timeseries.to_sdf().count()
    assert st_cnt == 210_384

    jt_cnt = ev.joined_timeseries.to_sdf().count()
    assert jt_cnt == 200_350

    m_cnt = ev.metrics.query(
        order_by=["primary_location_id", "month"],
        group_by=["primary_location_id", "month"],
        include_metrics=[
            teehr.DeterministicMetrics.KlingGuptaEfficiency(),
            teehr.DeterministicMetrics.NashSutcliffeEfficiency(),
            teehr.DeterministicMetrics.RelativeBias()
        ]
    ).to_sdf().count()
    assert m_cnt == 24

    ev.spark.stop()


if __name__ == "__main__":
    test_read_from_s3()