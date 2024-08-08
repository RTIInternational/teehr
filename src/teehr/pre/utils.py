"""Utility functions for the preprocessing."""
import geopandas as gpd
from pathlib import Path
from typing import Union
import logging

logger = logging.getLogger(__name__)


def read_spatial_file(
        filepath: Union[str, Path], **kwargs: str
) -> gpd.GeoDataFrame:
    """Load any supported geospatial file type into a gdf using GeoPandas."""
    logger.info(f"Reading spatial file {filepath}")
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


def validate_dataset_structure(dataset_filepath: Union[str, Path]) -> bool:
    """Validate the database structure."""
    if not Path(dataset_filepath).exists():
        return False

    if not Path(dataset_filepath).is_dir():
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
        if not Path(dataset_filepath, subdirectory).exists():
            logger.error(f"Subdirectory {subdirectory} not found.")
            return False

    return True


def merge_field_mappings(
        default_mapping: dict,
        custom_mapping: dict
) -> dict:
    """Merge the default mapping with the custom mapping.

    Parameters
    ----------
    default_mapping : dict
        The default mapping.
        Format: {input_field: output_field}
    custom_mapping : dict
        The custom mapping.
        Format: {input_field: output_field}

    Returns
    -------
    dict
        The merged mapping.

    Merges based on output field names.
    """
    default_mapping = default_mapping.copy()
    custom_mapping = custom_mapping.copy()
    default_mapping = {v: k for k, v in default_mapping.items()}
    custom_mapping = {v: k for k, v in custom_mapping.items()}
    default_mapping.update(custom_mapping)
    mapping = {v: k for k, v in default_mapping.items()}
    return mapping