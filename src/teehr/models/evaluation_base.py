"""Base model for Evaluation class."""
# from pydantic import BaseModel as PydanticBaseModel, ConfigDict


class EvaluationBase:
    """Base model for the Evaluation.

    Just to solve circular import issues?

    Could be used to validate Evaluation fields?
    """

    pass

    # model_config = ConfigDict(
    #     arbitrary_types_allowed=True,
    #     validate_assignment=True,
    #     extra='forbid'  # raise an error if extra fields are passed
    # )
