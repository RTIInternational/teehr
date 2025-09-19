"""TEEHR workflow model base configuration."""
import abc
from pydantic import BaseModel as PydanticBaseModel, ConfigDict

from teehr.models.evaluation_base import EvaluationBase


class WorkflowModelBase(PydanticBaseModel, abc.ABC):
    """Workflow model base configuration."""

    ev: EvaluationBase  # Evaluation

    @abc.abstractmethod
    def execute(self):
        """Execute the workflow."""
        pass

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        extra='forbid'  # raise an error if extra fields are passed
    )
