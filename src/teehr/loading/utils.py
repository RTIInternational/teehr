"""Utility functions for the preprocessing."""
import geopandas as gpd
import pandas as pd
import xarray as xr
from pathlib import Path
from typing import Union, List
import logging
import shutil
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


def _get_element_node_value(
    element: minidom.Element,
    tag_name: str
) -> str:
    """Get the node value of an element."""
    elements_list = element.getElementsByTagName(tag_name)
    if len(elements_list) == 0:
        logger.error(f"'{tag_name}' not found, unable to parse this file.")
        raise ValueError(
            f"'{tag_name}' not found, unable to parse this XML file."
        )
    elif len(elements_list) > 1:
        logger.error(
            f"More than one entry for'{tag_name}', unable to parse this file."
        )
        raise ValueError(
            f"More than one entry for'{tag_name}', unable to parse this file."
        )
    else:
        node_value = elements_list[0].firstChild.nodeValue
        return node_value


def _get_element_attribute_value(
    element: minidom.Element,
    tag_name: str,
    attribute_name: str
) -> str:
    """Get the attribute value of an element."""
    elements_list = element.getElementsByTagName(tag_name)
    if len(elements_list) == 0:
        logger.error(f"'{tag_name}' not found, unable to parse this file.")
        raise ValueError(
            f"'{tag_name}' not found, unable to parse this XML file."
        )
    elif len(elements_list) > 1:
        logger.error(
            f"More than one entry for'{tag_name}', unable to parse this file."
        )
        raise ValueError(
            f"More than one entry for'{tag_name}', unable to parse this file."
        )
    else:
        attr_value = elements_list[0].getAttribute(attribute_name)
        if len(attr_value) == 0:
            logger.error(
                f"Attribute '{attribute_name}' not found for '{tag_name}'."
            )
            raise ValueError(
                f"Attribute '{attribute_name}' not found for '{tag_name}'."
            )
        return attr_value


def read_and_convert_xml_to_df(
    in_filepath: Union[str, Path],
    field_mapping: dict,
) -> pd.DataFrame:
    """Read an xml file and convert to pandas dataframe."""
    logger.debug(f"Reading and converting xml file {in_filepath}")

    inv_field_mapping = {v: k for k, v in field_mapping.items()}

    location_id_kw = inv_field_mapping["location_id"]
    variable_name_kw = inv_field_mapping["variable_name"]
    reference_time_kw = inv_field_mapping["reference_time"]
    unit_name_kw = inv_field_mapping["unit_name"]
    member_kw = inv_field_mapping["member"]
    configuration_kw = inv_field_mapping["configuration_name"]

    fews_xml = minidom.parse(str(in_filepath))

    utc_offset = pd.Timedelta(
        float(
            fews_xml.getElementsByTagName("timeZone")[0].firstChild.nodeValue
        ),
        format="H"
    )
    series = fews_xml.getElementsByTagName("series")

    timeseries_list = []
    for member in series:
        location_id = _get_element_node_value(member, location_id_kw)
        variable_name = _get_element_node_value(member, variable_name_kw)
        configuration = _get_element_node_value(member, configuration_kw)
        unit_name = _get_element_node_value(member, unit_name_kw)
        ensemble_member = _get_element_node_value(member, member_kw)

        forecastDate = _get_element_attribute_value(member, reference_time_kw, "date")
        forecastTime = _get_element_attribute_value(member, reference_time_kw, "time")

        reference_time = pd.to_datetime(
            forecastDate + " " + forecastTime
        ) + utc_offset

        events = member.getElementsByTagName("event")
        timeseries_data = []
        for event in events:
            event_date = event.getAttribute("date")
            event_time = event.getAttribute("time")
            event_value = event.getAttribute("value")
            value_time = pd.to_datetime(
                event_date + " " + event_time
            ) + utc_offset
            timeseries_data.append({
                "value": event_value,
                "value_time": value_time
            })

        timeseries_df = pd.DataFrame(timeseries_data)
        timeseries_df["reference_time"] = reference_time
        timeseries_df["unit_name"] = unit_name
        timeseries_df["variable_name"] = variable_name
        timeseries_df["location_id"] = location_id
        timeseries_df["member"] = ensemble_member
        timeseries_df["configuration_name"] = configuration

        timeseries_list.append(timeseries_df)

    return pd.concat(timeseries_list)


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
