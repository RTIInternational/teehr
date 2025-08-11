"""Utility functions for the preprocessing."""
import geopandas as gpd
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Union, List
import logging
import shutil
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from lxml import etree
from datetime import datetime, timedelta
import pyarrow as pa
import pyogrio.errors


logger = logging.getLogger(__name__)

FEWS_XML_NAMESPACE = "{http://www.wldelft.nl/fews/PI}"


def add_or_replace_sdf_column_prefix(
    sdf: DataFrame,
    column_name: str,
    prefix: str,
    delimiter: str = "-",
) -> DataFrame:
    """Add or replace a column in a DataFrame with a prefix.

    Parameters
    ----------
    sdf : DataFrame
        The DataFrame to add or replace the column in.
    column_name : str
        The name of the column to add or replace the prefix.
    prefix : str
        The prefix to add or replace.
    """
    if len(
        sdf.select(column_name).first().asDict()[column_name].split("-")
    ) > 1:
        logger.debug(
            "ID already has a prefix. This will be replaced."
        )
        sdf = sdf.withColumn(
            column_name,
            F.concat(
                F.lit(prefix), F.lit("-"), F.split(F.col(column_name), delimiter).getItem(1)
            )
        )
    else:
        logger.debug(
            "ID does not have a prefix. This will be added."
        )
        sdf = sdf.withColumn(
            column_name,
            F.concat(F.lit(prefix), F.lit(delimiter), F.col(column_name))
        )
    return sdf


def read_spatial_file(
        filepath: Union[str, Path], **kwargs: str
) -> gpd.GeoDataFrame:
    """Load any supported geospatial file type into a gdf using GeoPandas."""
    # Define extension groups
    parquet_exts = {'.parquet', '.pq', '.parq', '.pqt'}
    feather_exts = {'.feather', '.ft'}
    file_exts = {'.gpkg', '.shp', '.geojson', '.json', '.gml', '.kml'}
    all_exts = parquet_exts.union(feather_exts).union(file_exts)

    # get extension
    ext = filepath.suffix.lower()

    if ext not in all_exts:
        raise ValueError(
            f"""Unsupported file extension: {ext}. Supported extensions are:
            {', '.join(all_exts)}"""
        )

    if isinstance(filepath, str):
        try:
            filepath = Path(filepath)
        except Exception:
            raise ValueError(
                """Invalid filepath provided. Conversion from 'str' to Path
                object failed."""
            )
    if not filepath.exists():
        raise FileNotFoundError(f"""The provided filepath does not exist:
                                {filepath}""")

    logger.debug(f"Loading geospatial file: {filepath}")

    if ext in parquet_exts:
        try:
            gdf = gpd.read_parquet(filepath, **kwargs)
        except (pa.lib.ArrowInvalid, pa.lib.ArrowIOError, ValueError) as e:
            raise ValueError(f"Failed to read parquet file: {e}")
    elif ext in feather_exts:
        try:
            gdf = gpd.read_feather(filepath, **kwargs)
        except (pa.lib.ArrowInvalid, pa.lib.ArrowIOError, ValueError) as e:
            raise ValueError(f"Failed to read feather file: {e}")
    elif ext in file_exts:
        try:
            gdf = gpd.read_file(filepath, **kwargs)
        except (pyogrio.errors.DataSourceError, ValueError, OSError) as e:
            raise ValueError(
                f"""Failed to read file with geopandas.read_file: {e}"""
                )
    else:
        raise ValueError(f"Unsupported spatial file extension: {ext}")

    if gdf.empty:
        raise ValueError(f"The file '{filepath}' was loaded but is empty.")
    if gdf.geometry is None or gdf.geometry.name not in gdf.columns:
        raise ValueError(
            f"The file '{filepath}' does not contain a valid geometry column."
            )
    if gdf.geometry.isnull().all():
        raise ValueError(
            f""""The file '{filepath}' contains only null geometries (possibly
            corrupt)."""
            )

    return gdf


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


def read_and_convert_xml_to_df(
    in_filepath: Union[str, Path],
    field_mapping: dict,
) -> pd.DataFrame:
    """Read a FEWS PI xml file and convert to pandas dataframe."""
    logger.debug(f"Reading and converting xml file {in_filepath}")

    inv_field_mapping = {v: k for k, v in field_mapping.items()}
    location_id_kw = inv_field_mapping["location_id"]
    variable_name_kw = inv_field_mapping["variable_name"]
    reference_time_kw = inv_field_mapping["reference_time"]
    unit_name_kw = inv_field_mapping["unit_name"]
    member_kw = inv_field_mapping["member"]
    configuration_kw = inv_field_mapping["configuration_name"]

    timeseries_data = []
    cntr = 0
    for _, series in etree.iterparse(in_filepath, tag=FEWS_XML_NAMESPACE + "series"):
        if cntr == 0:
            utc_offset = timedelta(
                hours=float(series.getparent().find(FEWS_XML_NAMESPACE + "timeZone").text)
            )
            cntr += 1

        # Get header info.
        location_id = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + location_id_kw).text
        variable_name = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + variable_name_kw).text
        configuration = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + configuration_kw).text
        unit_name = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + unit_name_kw).text
        missing_value = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + "missVal").text
        ensemble_member = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + member_kw).text
        forecastDate = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + reference_time_kw).get("date")
        forecastTime = series.find(FEWS_XML_NAMESPACE + "header/" + FEWS_XML_NAMESPACE + reference_time_kw).get("time")
        reference_time = datetime.strptime(
            forecastDate + " " + forecastTime, "%Y-%m-%d %H:%M:%S"
        )
        # Get timeseries data.
        events = series.findall(FEWS_XML_NAMESPACE + "event")
        for event in events:
            event_date = event.get("date")
            event_time = event.get("time")
            event_value = event.get("value")
            if event_value == missing_value:
                # TODO: Remove this when we have a better way to handle NaNs.
                logger.debug(
                    f"Missing value found in series for {location_id} at {event_date} {event_time}. Skipping."
                )
                event_value = None
                continue
            value_time = datetime.strptime(
                event_date + " " + event_time, "%Y-%m-%d %H:%M:%S"
            )
            timeseries_data.append({
                "value": event_value,
                "value_time": value_time,
                "reference_time": reference_time,
                "unit_name": unit_name,
                "variable_name": variable_name,
                "location_id": location_id,
                "member": ensemble_member,
                "configuration_name": configuration
            })
        series.clear()  # Clear the series element to free memory
    df = pd.DataFrame(timeseries_data)
    df["value_time"] = df.value_time + utc_offset
    df["reference_time"] = df.reference_time + utc_offset
    return df


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
