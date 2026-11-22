import json
import struct

import pyarrow as pa
import pytest

from teehr import wkb


def _point(x: float, y: float, order: str = "<") -> bytes:
    return struct.pack(order + "BIdd", 1 if order == "<" else 0, 1, x, y)


def _coords(*xy: tuple[float, float]) -> bytes:
    return struct.pack("<I", len(xy)) + b"".join(struct.pack("<dd", *p) for p in xy)


SQUARE = ((0, 0), (1, 0), (1, 1), (0, 0))
POLYGON = struct.pack("<BII", 1, 3, 1) + _coords(*SQUARE)


@pytest.mark.parametrize(
    ("value", "wkt"),
    [
        (_point(-69.7156, 46.7006), "POINT (-69.7156 46.7006)"),
        (_point(-69.7156, 46.7006, ">"), "POINT (-69.7156 46.7006)"),
        (struct.pack("<BIdd", 1, 1, float("nan"), float("nan")), "POINT EMPTY"),
        (struct.pack("<BIddd", 1, 1001, 1, 2, 3), "POINT Z (1 2 3)"),
        (struct.pack("<BI", 1, 2) + _coords((0, 0), (1, 2)), "LINESTRING (0 0, 1 2)"),
        (POLYGON, "POLYGON ((0 0, 1 0, 1 1, 0 0))"),
        (struct.pack("<BII", 1, 4, 2) + _point(0, 0) + _point(1, 1), "MULTIPOINT ((0 0), (1 1))"),
        (struct.pack("<BII", 1, 6, 1) + POLYGON, "MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)))"),
        (
            struct.pack("<BII", 1, 7, 2) + _point(0, 0) + POLYGON,
            "GEOMETRYCOLLECTION (POINT (0 0), POLYGON ((0 0, 1 0, 1 1, 0 0)))",
        ),
        (struct.pack("<BII", 1, 3, 0), "POLYGON EMPTY"),
    ],
)
def test_wkb_as_wkt(value, wkt):
    assert wkb.to_wkt(value) == wkt


def test_wkt_cut_to_a_width():
    table = pa.table({"geometry": pa.array([POLYGON], pa.binary())})
    assert wkb.with_wkt(table, 12)["geometry"][0].as_py() == "POLYGON ((0…"


def test_geoparquet_metadata_names_types_and_bounds():
    table = pa.table({
        "location_key": ["a", "b"],
        "geometry": pa.array([_point(-70, 46), POLYGON], pa.binary()),
        "bbox_xmin": [-70.0, 0.0], "bbox_ymin": [46.0, 0.0],
        "bbox_xmax": [-70.0, 1.0], "bbox_ymax": [46.0, 1.0],
    })  # fmt: skip
    geo = json.loads(wkb.with_geoparquet(table).schema.metadata[b"geo"])
    assert geo["version"] == "1.1.0" and geo["primary_column"] == "geometry"
    assert geo["columns"]["geometry"] == {
        "encoding": "WKB",
        "geometry_types": ["Point", "Polygon"],
        "bbox": [-70.0, 0.0, 1.0, 46.0],
    }


def test_no_geometry_column_is_left_alone():
    table = pa.table({"a": [1]})
    assert wkb.with_geoparquet(table) is table and wkb.with_wkt(table) is table
