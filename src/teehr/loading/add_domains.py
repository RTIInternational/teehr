"""Functions to add domain data to the dataset."""
from pathlib import Path
from typing import Union, List
import pandas as pd
import pandera as pa
from teehr.models.tables import (
    Configuration,
    Unit,
    Variable,
    Attribute,
)
import teehr.const as const
import logging

logger = logging.getLogger(__name__)


def add_configuration(
    dataset_path: Union[Path, str],
    configuration: Union[Configuration, List[Configuration]]
):
    """Insert a configuration into the dataset.

    The function reads the existing configurations from the dataset, validates
    the new configurations, and appends them to the dataset. The combined
    configurations are then validated against a predefined schema and saved
    back to the dataset.

    Parameters
    ----------
    dataset_path : Union[Path, str]
        Path to the dataset directory.
    configuration : Union[Configuration, List[Configuration]]
        Configuration object or list of Configuration objects to add to the

    Returns
    -------
    None
    """
    filepath = Path(
        dataset_path, const.CONFIGURATIONS_DIR, const.CONFIGURATIONS_FILE
    )
    logger.debug(f"Adding configuration to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Configuration.model_fields]]

    if isinstance(configuration, Configuration):
        configuration = [configuration]

    new_df = pd.DataFrame([c.model_dump() for c in configuration])
    new_df = new_df[[key for key in Configuration.model_fields]]

    schema = pa.DataFrameSchema(
        columns={
            "name": pa.Column(
                pa.String,
                unique=True,
                checks=[
                    pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                ],
                regex=r"^[a-zA-Z0-9_]+$"
            ),
            "type": pa.Column(
                pa.String,
                checks=pa.Check.isin(
                    ["primary", "secondary"]
                )
            ),
            "description": pa.Column(
                pa.String,
                checks=pa.Check(lambda s: not s.str.contains(",").any())
            ),
        },
        strict="filter"
    )

    combined_df = pd.concat([org_df, new_df])
    validated_df = schema.validate(combined_df)
    validated_df.to_csv(filepath, index=False)


def add_unit(
    dataset_path: Union[Path, str],
    unit: Union[Unit, List[Unit]]
):
    """Insert a unit into the dataset.

    The function reads the existing units from the dataset, validates the new
    units, and appends them to the dataset. The combined units are then
    validated against a predefined schema and saved back to the dataset.

    Parameters
    ----------
    dataset_path : Union[Path, str]
        Path to the dataset directory.
    unit : Union[Unit, List[Unit]]
        Unit object or list of Unit objects to add to the dataset.

    Returns
    -------
    None
    """
    filepath = Path(dataset_path, const.UNITS_DIR, const.UNITS_FILE)
    logger.debug(f"Adding unit to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Unit.model_fields]]

    if isinstance(unit, Unit):
        unit = [unit]

    new_df = pd.DataFrame([u.model_dump() for u in unit])
    new_df = new_df[[key for key in Unit.model_fields]]

    schema = pa.DataFrameSchema(
        columns={
            "name": pa.Column(
                pa.String,
                unique=True,
                checks=[
                    pa.Check.str_matches(r"^[a-zA-Z0-9_^/]+$")
                ]
            ),
            "long_name": pa.Column(
                pa.String,
                checks=pa.Check(lambda s: not s.str.contains(",").any())
            ),
        },
        strict="filter"
    )

    combined_df = pd.concat([org_df, new_df])
    validated_df = schema.validate(combined_df)
    validated_df.to_csv(filepath, index=False)


def add_variable(
    dataset_path: Union[Path, str],
    variable: Union[Variable, List[Variable]]
):
    """Insert a variable into the dataset.

    The function reads the existing variables from the dataset, validates the
    new variables, and appends them to the dataset. The combined variables are
    then validated against a predefined schema and saved back to the dataset.

    Parameters
    ----------
    dataset_path : Union[Path, str]
        Path to the dataset directory.
    variable : Union[Variable, List[Variable]]
        Variable object or list of Variable objects to add to the dataset.

    Returns
    -------
    None
    """
    filepath = Path(dataset_path, const.VARIABLES_DIR, const.VARIABLES_FILE)
    logger.debug(f"Adding variable to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Variable.model_fields]]

    if isinstance(variable, Variable):
        variable = [variable]

    new_df = pd.DataFrame([u.model_dump() for u in variable])
    new_df = new_df[[key for key in Variable.model_fields]]

    schema = pa.DataFrameSchema(
        columns={
            "name": pa.Column(
                pa.String,
                unique=True,
                checks=[
                    pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                ],
                regex=r"^[a-zA-Z0-9_]+$"
            ),
            "long_name": pa.Column(
                pa.String,
                checks=pa.Check(lambda s: not s.str.contains(",").any())
            ),
        },
        strict="filter"
    )

    combined_df = pd.concat([org_df, new_df])
    validated_df = schema.validate(combined_df)
    validated_df.to_csv(filepath, index=False)


def add_attribute(
    dataset_path: Union[Path, str],
    attribute: Union[Attribute, List[Attribute]]
):
    """Insert an attribute into the dataset.

    The function reads the existing attributes from the dataset, validates the
    new attributes, and appends them to the dataset. The combined attributes
    are then validated against a predefined schema and saved back to the
    dataset.

    Parameters
    ----------
    dataset_path : Union[Path, str]
        Path to the dataset directory.
    attribute : Union[Attribute, List[Attribute]]
        Attribute object or list of Attribute objects to add to the dataset.

    Returns
    -------
    None
    """
    filepath = Path(dataset_path, const.ATTRIBUTES_DIR, const.ATTRIBUTES_FILE)
    logger.debug(f"Adding attribute to {filepath}")

    org_df = pd.read_csv(filepath)
    org_df = org_df[[key for key in Attribute.model_fields]]

    if isinstance(attribute, Attribute):
        attribute = [attribute]

    new_df = pd.DataFrame([u.model_dump() for u in attribute])
    new_df = new_df[[key for key in Attribute.model_fields]]

    schema = pa.DataFrameSchema(
        columns={
            "name": pa.Column(
                pa.String,
                unique=True,
                checks=[
                    pa.Check.str_matches(r"^[a-zA-Z0-9_]+$")
                ],
            ),
            "type": pa.Column(
                pa.String,
                checks=pa.Check.isin(
                    ["categorical", "continuous"]
                )
            ),
            "description": pa.Column(
                pa.String,
                checks=pa.Check(lambda s: not s.str.contains(",").any())
            ),
        },
        strict="filter"
    )

    combined_df = pd.concat([org_df, new_df])
    validated_df = schema.validate(combined_df)
    validated_df.to_csv(filepath, index=False)
