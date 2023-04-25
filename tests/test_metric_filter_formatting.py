from datetime import datetime

import teehr.models.queries as tmq
import teehr.queries.duckdb as tqd


def test_multiple_filters():
    filter_1 = tmq.JoinedFilter(
        column="secondary_location_id",
        operator="in",
        value=["123456", "9876543"]
    )
    filter_2 = tmq.JoinedFilter(
        column="reference_time",
        operator="=",
        value=datetime(2023, 1, 1, 0, 0, 0)
    )
    filter_str = tqd.filters_to_sql([filter_1, filter_2])
    assert filter_str == "WHERE secondary_location_id in ('123456','9876543') AND reference_time = '2023-01-01 00:00:00'"  # noqa


def test_no_filters():
    filter_str = tqd.filters_to_sql([])
    assert filter_str == "--no where clause"


if __name__ == "__main__":
    test_multiple_filters()
    test_no_filters()
    pass
