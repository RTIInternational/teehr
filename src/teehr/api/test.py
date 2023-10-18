from fastapi.testclient import TestClient
from main import app

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
        "include_geometry": True
    }

    response = client.post(
        "/datasets/study-a/get_metrics",
        json=post
    )
    assert response.status_code == 200
    print(response.json())


def test_get_metrics_filter():

    post = {
        "group_by": ["primary_location_id"],
        "order_by": ["primary_location_id"],
        "include_metrics": "all",
        "return_query": False,
        "include_geometry": False,
        "filter": [{
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


def test_get_fields():
    response = client.get(
        "/datasets/{dataset_id}/get_metric_fields"
    )
    # assert response.status_code == 200
    print(response.json())


if __name__ == "__main__":
    # test_read_root()
    test_read_datasets()
    # test_get_metrics()
    # test_get_metrics_filter()
    # test_get_fields()
