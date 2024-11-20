"""Utility functions for the preprocessing."""
import geopandas as gpd
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Union, List
import logging
import shutil
import xml.etree.ElementTree as ET
from xml.dom import minidom


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


def validate_constant_values_dict(
    constant_values_dict: dict,
    fields: List[str],
):
    """Validate the values of the constants."""
    constant_keys = constant_values_dict.keys()
    for key in constant_keys:
        if key not in fields:
            logger.error(
                f"Invalid constant value key: {key}"
                f"Valid keys are: {fields}"
            )
            raise ValueError(f"Invalid constant value key: {key}")

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
    default_values = set(default_mapping.values())
    custom_values = set(custom_mapping.values())
    if not custom_values.issubset(default_values):
        logger.error(
            f"All custom values must be in default values: {default_values}"
        )
        raise ValueError("All custom values must be in default values")

    default_mapping = default_mapping.copy()
    custom_mapping = custom_mapping.copy()
    default_mapping = {v: k for k, v in default_mapping.items()}
    custom_mapping = {v: k for k, v in custom_mapping.items()}

    default_mapping.update(custom_mapping)
    mapping = {v: k for k, v in default_mapping.items()}
    return mapping


def copy_template_to(
        template_dir: Union[str, Path],
        destination_dir: Union[str, Path]
):
    """Copy the template directory to the destination directory."""
    template_dir = Path(template_dir)
    destination_dir = Path(destination_dir)

    for file in template_dir.glob('**/*'):
        if file.is_dir():
            destination_file = Path(
                destination_dir,
                file.relative_to(template_dir)
            )
            if not destination_file.parent.is_dir():
                destination_file.parent.mkdir(parents=True)
            logger.debug(f"Making directory {destination_file}")
            destination_file.mkdir()
        if file.is_file():
            destination_file = Path(
                destination_dir,
                file.relative_to(template_dir)
            )
            if not destination_file.parent.is_dir():
                destination_file.parent.mkdir(parents=True)
            logger.debug(f"Copying file {file} to {destination_file}")
            shutil.copy(file, destination_file)

    logger.debug(
        f"Renaming {destination_dir}/gitignore_template to .gitignore"
    )

    gitignore_text = Path(destination_dir, "gitignore_template")
    gitignore_text.rename(Path(destination_dir, ".gitignore"))


def convert_datetime_ns_to_ms(
    df: pd.DataFrame
) -> pd.DataFrame:
    """Convert datetime columns from nanoseconds to milliseconds."""
    for column in df.columns:
        if df[column].dtype == "datetime64[ns]":
            df[column] = df[column].astype("datetime64[ms]")
    return df


def read_and_convert_netcdf_to_df(
        in_filepath: Union[str, Path],
        field_mapping: dict,
        **kwargs
) -> pd.DataFrame:
    """Read a netcdf file and convert to pandas dataframe."""
    logger.debug(f"Reading and converting netcdf file {in_filepath}")
    with xr.open_dataset(in_filepath, **kwargs) as ds:
        # Get only the fields that are included in the field mapping.
        field_list = [field for field in field_mapping if field in ds]
        df = ds[field_list].to_dataframe()
    df.reset_index(inplace=True)
    return df


def strip_namespace(tag):
    """Strip the namespace from the tag."""
    return tag.split('}', 1)[-1] if '}' in tag else tag


def read_and_convert_xml_to_df(
        in_filepath: Union[str, Path],
        field_mapping: dict,
        **kwargs
) -> pd.DataFrame:
    """Read a xml file and convert to pandas dataframe.

    Parameters
    ----------
    in_filepath : Union[str, Path]
        The input xml file.
    field_mapping : dict
        The field mapping.
    **kwargs
        Keywords for parsing the XML file. Currently only handles the
        namespace keyword.

    Returns
    -------
    pd.DataFrame

    Notes
    -----
    This function adheres to the Delft-FEWS Published Interface (PI)
    XML format.

    reference: https://publicwiki.deltares.nl/display/FEWSDOC/Dynamic+data
    """
    logger.debug(f"Reading and converting xml file {in_filepath}")

    tree = ET.parse(in_filepath)
    root = tree.getroot()
    namespace = {"xmlns": kwargs.get("namespace")}

    # This is the offset from UTC in decimal hours.
    utc_offset = float(root.find("xmlns:timeZone", namespace).text)

    timeseries_df_list = []
    series_tags = root.findall("xmlns:series", namespace)
    for series in series_tags:
        timeseries_data = []
        events = series.findall("xmlns:event", namespace)
        for event in events:
            date = event.get("date")
            time = event.get("time")
            value = event.get("value")
            timeseries_data.append(
                {"date": date, "time": time, "value": value}
            )
        timeseries_df = pd.DataFrame(timeseries_data)
        # Combine date and time columns and apply the UTC offset.
        timeseries_df["value_time"] = pd.to_datetime(
            timeseries_df["date"] + " " + timeseries_df["time"]
        ) + pd.Timedelta(utc_offset, format="H")
        # Add header info.
        header = series.find("xmlns:header", namespace)
        if header is not None:
            for child in header:
                timeseries_df.loc[:, strip_namespace(child.tag)] = child.text
        # Parse out the columns specified in the field mapping
        col_subset = [
            col for col in field_mapping if col in timeseries_df.columns
        ]
        timeseries_df_list.append(timeseries_df[col_subset])

    return pd.concat(timeseries_df_list)


def validate_input_is_parquet(
        in_path: Union[str, Path]
) -> None:
    """
    Validate that the input is parquet format.

    Check that either the file is a parquet file or the directory
    contains parquet files.
    """
    in_path = Path(in_path)
    if in_path.is_dir():
        if len(list(in_path.glob("**/*.parquet"))) == 0:
            logger.error("No parquet files found in the directory.")
            raise ValueError("No parquet files found in the directory.")
    else:
        if not in_path.suffix.endswith(".parquet"):
            logger.error("Input file must be a parquet file.")
            raise ValueError("Input file must be a parquet file.")


def validate_input_is_csv(
        in_path: Union[str, Path]
) -> None:
    """
    Validate that the input is csv format.

    Check that either the file is a csv file or the directory
    contains csv files.
    """
    in_path = Path(in_path)
    if in_path.is_dir():
        if len(list(in_path.glob("**/*.csv"))) == 0:
            logger.error("No csv files found in the directory.")
            raise ValueError("No csv files found in the directory.")
    else:
        if not in_path.suffix.endswith(".csv"):
            logger.error("Input file must be a csv file.")
            raise ValueError("Input file must be a csv file.")


def validate_input_is_netcdf(
        in_path: Union[str, Path]
) -> None:
    """
    Validate that the input is netcdf format.

    Check that either the file is a netcdf file or the directory
    contains netcdf files.
    """
    in_path = Path(in_path)
    if in_path.is_dir():
        if len(list(in_path.glob("**/*.nc"))) == 0:
            logger.error("No netcdf files found in the directory.")
            raise ValueError("No netcdf files found in the directory.")
    else:
        if not in_path.suffix.endswith(".nc"):
            logger.error("Input file must be a netcdf file.")
            raise ValueError("Input file must be a netcdf file.")


def validate_input_is_xml(
        in_path: Union[str, Path]
) -> None:
    """
    Validate that the input is xml format.

    Check that either the file is a xml file or the directory
    contains xml files.
    """
    in_path = Path(in_path)
    if in_path.is_dir():
        if len(list(in_path.glob("**/*.xml"))) == 0:
            logger.error("No xml files found in the directory.")
            raise ValueError("No xml files found in the directory.")
    else:
        if not in_path.suffix.endswith(".xml"):
            logger.error("Input file must be an xml file.")
            raise ValueError("Input file must be an xml file.")
