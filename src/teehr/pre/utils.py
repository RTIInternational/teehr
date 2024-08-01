import geopandas as gpd
from pathlib import Path
from typing import Union
import logging

logger = logging.getLogger(__name__)


def read_spatial_file(filepath: Union[str, Path], **kwargs: str) -> gpd.GeoDataFrame:
    """Load any supported geospatial file type into a gdf using GeoPandas."""
    try:
        gdf = gpd.read_file(filepath, **kwargs)
        return gdf
    except Exception:
        pass
    try:
        gdf = gpd.read_parquet(filepath, **kwargs)
        return gdf
    except Exception:
        pass
    try:
        gdf = gpd.read_feather(filepath, **kwargs)
        return gdf
    except Exception:
        logger.error(f"Unsupported file type {filepath}")
        raise Exception("Unsupported file type")


def validate_database_structure(database_filepath: Union[str, Path]) -> bool:
    """Validate the database structure."""
    if not Path(database_filepath).exists():
        return False

    if not Path(database_filepath).is_dir():
        return False

    subdirectories = [
        "attributes",
        "configurations",
        "joined_timeseries",
        "location_crosswalks",
        "location_attributes",
        "locations",
        "primary_timeseries",
        "secondary_timeseries",
        "units",
        "variables",
    ]
    for subdirectory in subdirectories:
        if not Path(database_filepath, subdirectory).exists():
            logger.error(f"Subdirectory {subdirectory} not found.")
            return False

    return True


