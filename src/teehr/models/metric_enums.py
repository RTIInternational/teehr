"""Enums for metrics models."""
try:
    # breaking change introduced in python 3.11
    from enum import StrEnum
except ImportError:  # pragma: no cover
    from enum import Enum  # pragma: no cover

    class StrEnum(str, Enum):  # pragma: no cover
        """Enum with string values."""

        pass  # pragma: no cover


class Operators(StrEnum):
    """Operators for filters."""

    eq = "="
    gt = ">"
    lt = "<"
    gte = ">="
    lte = "<="
    islike = "like"
    isin = "in"


class ValueTypeEnum(StrEnum):
    """Value types."""

    Deterministic = "Deterministic"
    Categorical = "Categorical"
    Signature = "Signature"
    Probabilistic = "Probabilistic"
    Time = "Time"


class BootstrapMethodEnum(StrEnum):
    """Bootstrap methods."""

    percentile = "percentile"
    bias_corrected = "bias_corrected"
    bias_corrected_percentile = "bias_corrected_percentile"
    t = "t"
    pivot = "pivot"


class TransformEnum(StrEnum):
    """Transform methods."""

    log = "log"
    sqrt = "sqrt"
    square = "square"
    cube = "cube"
    exp = "exp"
    inv = "inv"
    abs = "abs"
    none = "none"