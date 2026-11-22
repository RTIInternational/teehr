"""WKB geometry without shapely (ADR-0015 keeps the client light).

WKT for printing, and GeoParquet metadata for files the CLI writes. Layer and result
geometry is WKB in EPSG:4326 (layers-and-crosswalks.md).
"""

import json
import struct
from typing import Any

import pyarrow as pa

#: ISO WKB type codes (the last three digits; 1000s add Z, M, ZM).
_TYPES = {
    1: "Point",
    2: "LineString",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}
_EWKB_Z, _EWKB_M, _EWKB_SRID = 0x80000000, 0x40000000, 0x20000000


class _Reader:
    def __init__(self, data: bytes) -> None:
        self.data, self.pos = data, 0

    def read(self, fmt: str) -> tuple[Any, ...]:
        values = struct.unpack_from(fmt, self.data, self.pos)
        self.pos += struct.calcsize(fmt)
        return values

    def header(self) -> tuple[str, int, int]:
        """(byte order, base type, dimensions) of the next geometry."""
        order = "<" if self.read("B")[0] == 1 else ">"
        (code,) = self.read(order + "I")
        z = bool(code & _EWKB_Z) or (code & 0xFFFF) // 1000 in (1, 3)
        m = bool(code & _EWKB_M) or (code & 0xFFFF) // 1000 in (2, 3)
        if code & _EWKB_SRID:
            self.read(order + "I")
        return order, (code & 0xFFFF) % 1000, 2 + z + m


def _fmt(value: float) -> str:
    return f"{value:.15g}"


def _geometry(r: _Reader, tagged: bool = True) -> str:
    """Read the next geometry as WKT.

    ``tagged=False`` leaves out its type name (a member of a multi-geometry).
    """
    order, kind, dims = r.header()
    name = _TYPES.get(kind)
    if name is None:
        raise ValueError(f"unknown WKB geometry type {kind}")

    def count() -> int:
        return int(r.read(order + "I")[0])

    def coords() -> str:
        return " ".join(_fmt(v) for v in r.read(order + "d" * dims))

    def listed(items: list[str]) -> str:
        return "(" + ", ".join(items) + ")" if items else "EMPTY"

    if kind == 1:
        values = r.read(order + "d" * dims)
        empty = all(v != v for v in values)  # NaN coordinates: an empty point
        text = "EMPTY" if empty else "(" + " ".join(_fmt(v) for v in values) + ")"
    elif kind == 2:
        text = listed([coords() for _ in range(count())])
    elif kind == 3:
        text = listed(
            ["(" + ", ".join(coords() for _ in range(count())) + ")" for _ in range(count())]
        )
    else:  # multi-geometries name their members' type once; a collection each time
        text = listed([_geometry(r, tagged=kind == 7) for _ in range(count())])
    if not tagged:
        return text
    return name.upper() + {2: "", 3: " Z", 4: " ZM"}[dims] + " " + text


def to_wkt(value: bytes) -> str:
    """Convert a WKB geometry to WKT."""
    return _geometry(_Reader(value))


def geometry_type(value: bytes) -> str:
    """Name a WKB geometry's type as GeoParquet does, e.g. "Point" or "Polygon Z"."""
    _, kind, dims = _Reader(value).header()
    return _TYPES.get(kind, "Unknown") + {2: "", 3: " Z", 4: " ZM"}[dims]


def is_geometry(table: pa.Table) -> bool:
    """Say whether the table has a WKB ``geometry`` column (a layer, or places)."""
    return "geometry" in table.column_names and pa.types.is_binary(table["geometry"].type)


def with_wkt(table: pa.Table, width: int | None = None) -> pa.Table:
    """Turn the table's WKB geometry column into WKT text.

    Each is cut to ``width`` characters (with "…") when given.
    """
    if not is_geometry(table):
        return table

    def text(value: bytes) -> str:
        wkt = to_wkt(value)
        return wkt if width is None or len(wkt) <= width else wkt[: width - 1] + "…"

    index = table.column_names.index("geometry")
    wkt = [None if v is None else text(v) for v in table["geometry"].to_pylist()]
    return table.set_column(index, "geometry", pa.array(wkt, pa.string()))


def with_geoparquet(table: pa.Table) -> pa.Table:
    """Add GeoParquet 1.1 metadata, so GeoPandas and other readers see the geometry.

    No "crs" means OGC:CRS84 (EPSG:4326, longitude/latitude).
    """
    if not is_geometry(table):
        return table
    values = [v for v in table["geometry"].to_pylist() if v is not None]
    column: dict[str, Any] = {
        "encoding": "WKB",
        "geometry_types": sorted({geometry_type(v) for v in values}),
    }
    bounds = ("bbox_xmin", "bbox_ymin", "bbox_xmax", "bbox_ymax")
    if values and all(b in table.column_names for b in bounds):
        import pyarrow.compute as pc

        xmin, ymin, xmax, ymax = (table[b] for b in bounds)
        column["bbox"] = [
            pc.min(xmin).as_py(), pc.min(ymin).as_py(), pc.max(xmax).as_py(), pc.max(ymax).as_py()
        ]  # fmt: skip
    geo = {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": column}}
    return table.replace_schema_metadata(
        {**(table.schema.metadata or {}), b"geo": json.dumps(geo).encode()}
    )
