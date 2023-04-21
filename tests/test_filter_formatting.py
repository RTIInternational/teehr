from datetime import datetime

import pytest
import teehr.models.queries as tmq
from pydantic import ValidationError
import teehr.queries.duckdb as tqd


def test_filter_string():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator="=",
        value="123456"
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id = '123456'"


def test_filter_int():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator="=",
        value=123456
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id = 123456"


def test_filter_int_gte():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator=">=",
        value=123456
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id >= 123456"


def test_filter_int_lt():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator="<",
        value=123456
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id < 123456"


def test_filter_float():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator="=",
        value=123.456
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id = 123.456"


def test_filter_datetime():
    filter = tmq.Filter(
        column="reference_time",
        operator="=",
        value=datetime(2023, 4, 1, 23, 30)
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "reference_time = '2023-04-01 23:30:00'"


def test_in_filter_string_wrong_operator():
    with pytest.raises(ValidationError):
        filter = tmq.Filter(
            column="secondary_location_id",
            operator="=",
            value=["123456", "9876"]
        )
        tqd.format_filter_item(filter)


def test_in_filter_string_wrong_value_type():
    with pytest.raises(ValidationError):
        filter = tmq.Filter(
            column="secondary_location_id",
            operator="in",
            value="9876"
        )
        tqd.format_filter_item(filter)


def test_in_filter_string():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator="in",
        value=["123456", "9876"]
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id in ('123456','9876')"


def test_in_filter_int():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator="in",
        value=[123456, 9876]
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id in (123456,9876)"


def test_in_filter_float():
    filter = tmq.Filter(
        column="secondary_location_id",
        operator="in",
        value=[123.456, 98.76]
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "secondary_location_id in (123.456,98.76)"


def test_in_filter_datetime():
    filter = tmq.Filter(
        column="reference_time",
        operator="in",
        value=[datetime(2023, 4, 1, 23, 30), datetime(2023, 4, 2, 23, 30)]
    )
    filter_str = tqd.format_filter_item(filter)
    assert filter_str == "reference_time in ('2023-04-01 23:30:00','2023-04-02 23:30:00')"  # noqa


if __name__ == "__main__":
    test_filter_string()
    test_filter_int()
    test_filter_int_gte()
    test_filter_int_lt()
    test_filter_float()
    test_filter_datetime()
    test_in_filter_string_wrong_operator()
    test_in_filter_string_wrong_value_type()
    test_in_filter_string()
    test_in_filter_int()
    test_in_filter_float()
    test_in_filter_datetime()
    pass
