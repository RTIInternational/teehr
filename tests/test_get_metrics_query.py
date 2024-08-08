"""Test evaluation class."""
from teehr import Evaluation, Metrics, Bootstrap
from teehr import Operators as ops


def test_get_metrics():
    """Test get_metrics method."""
    # Define the evaluation object.
    eval = Evaluation(dir_path="/home/sam/temp/temp_study_template")

    # Define the metrics to include.
    boot = Bootstrap(method="bias_corrected", num_samples=100)
    kge = Metrics.KlingGuptaEfficiency(bootstrap=boot)
    include_metrics = [kge, Metrics.RootMeanSquareError()]

    # Get the currently available fields to use in the query.
    flds = eval.fields

    # Define some filters.
    filters = [
        {
            "column": flds.primary_location_id,
            "operator": ops.eq,
            "value": "gage-A",
        },
        {
            "column": flds.reference_time,
            "operator": ops.eq,
            "value": "2022-01-01 00:00:00",
        }
    ]

    eval.get_metrics(
        group_by=[flds.primary_location_id],
        order_by=[flds.primary_location_id],
        include_metrics=include_metrics,
        filters=filters,
        include_geometry=True,
        return_query=False,
    )

    pass


if __name__ == "__main__":
    test_get_metrics()
