"""Field enums for each table in the dataset."""
try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:  # pragma: no cover
    from enum import Enum  # pragma: no cover

    class StrEnum(str, Enum):  # pragma: no cover
        """Enum with string values."""

        pass  # pragma: no cover


class ConfigurationFields(StrEnum):
    """Empty class for ConfigurationFieldEnum."""

    pass


class UnitFields(StrEnum):
    """Empty class for UnitFieldEnum."""

    pass


class VariableFields(StrEnum):
    """Empty class for VariableFieldEnum."""

    pass


class AttributeFields(StrEnum):
    """Empty class for AttributeFieldEnum."""

    pass


class LocationFields(StrEnum):
    """Empty class for LocationFieldEnum."""

    pass


class LocationAttributeFields(StrEnum):
    """Empty class for LocationAttributeFieldEnum."""

    pass


class LocationCrosswalkFields(StrEnum):
    """Empty class for LocationCrosswalkFieldEnum."""

    pass


class TimeseriesFields(StrEnum):
    """Empty class for TimeseriesFieldEnum."""

    pass


class JoinedTimeseriesFields(StrEnum):
    """Empty class for JoinedTimeseriesFieldEnum."""

    pass
