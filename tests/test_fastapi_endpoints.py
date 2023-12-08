from fastapi.testclient import TestClient
from teehr.api.main import app

client = TestClient(app)


def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Welcome to TEEHR"}


def test_read_datasets():
    response = client.get("/datasets/")
    assert response.status_code == 200
    print(response.json())


def test_get_metrics():

    post = {
        "group_by": ["primary_location_id"],
        "order_by": ["primary_location_id"],
        "include_metrics": "primary_count",
        "return_query": False,
        "include_geometry": False
    }

    response = client.post(
        "/datasets/study-a/get_metrics",
        json=post
    )
    print(response.json())
    assert response.status_code == 200
    # To unpack error msg from pydantic (only present if fails?)
    # print(response.json()["detail"][0]["msg"])


def test_get_metrics_filter():

    post = {
        "group_by": ["primary_location_id"],
        "order_by": ["primary_location_id"],
        "include_metrics": "all",
        "return_query": False,
        "include_geometry": False,
        "filters": [{
            "column": "primary_location_id",
            "operator": "=",
            "value": "gage-A"
        }]
    }

    response = client.post(
        "/datasets/study-a/get_metrics",
        json=post
    )
    assert response.status_code == 200
    print(response.json())


def test_get_metric_fields():
    response = client.get(
        "/datasets/study-a/get_metric_fields"
    )
    assert response.status_code == 200
    print(response.json())


def test_get_data_fields():
    response = client.get(
        "/datasets/study-a/get_data_fields"
    )
    assert response.status_code == 200
    print(response.json())


def test_get_timeseries():

    post = {
        "order_by": ["primary_location_id"],
        "return_query": False,
        "filters": [{
            "column": "primary_location_id",
            "operator": "=",
            "value": "gage-A"
        }],
        "timeseries_name": "primary"
    }

    response = client.post(
        "/datasets/study-a/get_timeseries",
        json=post
    )
    assert response.status_code == 200
    print(response.json())


def test_get_timeseries_chars():

    # post = {
    #     "order_by": ["primary_location_id"],
    #     "return_query": False,
    #     "filters": [{
    #         "column": "primary_location_id",
    #         "operator": "=",
    #         "value": "gage-A"
    #     }]
    # }

    post = {
        "filters": [],
        "group_by": ["primary_location_id"],
        "order_by": ["primary_location_id"],
        "timeseries_name": "primary",
        # "return_query": False
    }

    response = client.post(
        "/datasets/study-a/get_timeseries_chars",
        json=post
    )
    assert response.status_code == 200
    print(response.json())


def test_get_unique_field_values():
    post = {
        "field_name": "primary_location_id"
    }
    response = client.post(
        "/datasets/study-a/get_unique_field_values",
        json=post
    )
    assert response.status_code == 200
    print(response.json())


if __name__ == "__main__":
    # test_read_root()
    # test_read_datasets()
    # test_get_metrics()
    # test_get_metrics_filter()
    # test_get_metric_fields()
    # test_get_data_fields()
    # test_get_timeseries()
    test_get_timeseries_chars()
    # test_get_unique_field_values()
