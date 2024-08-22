"""Classes for bootstrapping sampling methods."""
from typing import Dict, Union
from arch.bootstrap import (
    StationaryBootstrap,
    CircularBlockBootstrap,
    IIDBootstrap
)
from arch.typing import ArrayLike, Int64Array
from numpy.random import Generator, RandomState
import numpy as np

from pydantic import BaseModel as PydanticBaseModel
from pydantic import ConfigDict


class BootstrapBasemodel(PydanticBaseModel):
    """Metrics Basemodel configuration."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
        # extra='forbid'  # raise an error if extra fields are passed
    )


def _get_random_integers(
    prng: Generator | RandomState, upper: int, *, size: int = 1
) -> Int64Array:  # type: ignore
    """From the arch package."""
    if isinstance(prng, Generator):
        return prng.integers(upper, size=size, dtype=np.int64)
    else:
        assert isinstance(prng, RandomState)
        return prng.randint(upper, size=size, dtype=np.int64)


class GumBootsBase(IIDBootstrap):
    """Custom implementation inheriting IIDBootstrap from the arch package."""

    def __init__(
        self,
        block_size: int,
        *args: ArrayLike,
        random_state: RandomState | None = None,
        seed: None | int | Generator | RandomState = None,
        **kwargs: ArrayLike,
    ) -> None:
        """Initialize the GumBoots class."""
        super().__init__(*args, random_state=random_state, seed=seed, **kwargs)
        self.block_size: int = block_size
        self._parameters = [block_size]

    def update_indices(self) -> Int64Array:  # type: ignore
        """TODO: This will be the GumBoots implementation."""
        num_blocks = self._num_items // self.block_size
        if num_blocks * self.block_size < self._num_items:
            num_blocks += 1
        indices = _get_random_integers(
            self._generator, self._num_items, size=num_blocks
        )
        indices = indices[:, None] + np.arange(self.block_size)
        indices = indices.flatten()
        indices %= self._num_items

        if indices.shape[0] > self._num_items:
            return indices[: self._num_items]
        else:
            return indices


class GumBoots(BootstrapBasemodel):
    """Base class for bootstrapping sampling methods."""

    bootstrapper: IIDBootstrap = GumBootsBase
    reps: int = 1000
    seed: int = 42
    random_state: RandomState | None = None
    args: ArrayLike | None = []
    kwargs: ArrayLike | None = None
    # waterYearMonth = 10,
    # startYear = NULL,
    # endYear = NULL,
    # minDays = 100,
    # minYears = 10,


class CircularBlock(BootstrapBasemodel):
    """Base class for bootstrapping sampling methods."""

    bootstrapper: IIDBootstrap = CircularBlockBootstrap
    kwargs: Dict[str, Union[int, RandomState, None]] = {"seed": 42, "random_state": None}
    reps: int = 1000
    block_size: int = 365
    args_arch: ArrayLike | None = []  # positional arguments passed to CircularBlockBootstrap.bootstrap
    kwargs_arch: ArrayLike | None = None # keyword arguments passed to CircularBlockBootstrap.bootstrap


class Bootstrappers:
    """Container class for bootstrap sampling classes."""

    GumBoots = GumBoots
    CircularBlock = CircularBlock
    # StationaryBootstrap = StationaryBootstrap
