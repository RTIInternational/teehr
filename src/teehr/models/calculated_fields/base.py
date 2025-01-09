import abc
from pydantic import BaseModel as PydanticBaseModel, ConfigDict


class CalculatedFieldABC(abc.ABC):
    @abc.abstractmethod
    def apply_to(self):
        pass


class CalculatedFieldBaseModel(PydanticBaseModel):
    """Calculated field base model configuration."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )