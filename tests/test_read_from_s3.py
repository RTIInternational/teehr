import teehr

def test_read_from_s3():
    # path = "s3a://ciroh-rti-public-data/teehr-data-warehouse/v0_4_evaluations/e1_camels_daily_streamflow"
    path = "s3a://ciroh-rti-public-data/teehr-data-warehouse/v0_4_evaluations/e0_2_location_example"

    ev = teehr.Evaluation(dir_path=path)

    ev.units.to_sdf().show(2)
    ev.primary_timeseries.to_sdf().show(2)
    ev.secondary_timeseries.to_sdf().show(2)
    ev.joined_timeseries.to_sdf().show(2)
    ev.metrics.query(
        order_by=["primary_location_id", "month"],
        group_by=["primary_location_id", "month"],
        include_metrics=[
            teehr.Metrics.KlingGuptaEfficiency(),
            teehr.Metrics.NashSutcliffeEfficiency(),
            teehr.Metrics.RelativeBias()
        ]
    ).to_sdf().show()


if __name__ == "__main__":
    test_read_from_s3()
    print("All tests passed")