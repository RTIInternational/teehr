"""Bootstrap classes."""
from arch.bootstrap import (
    IIDBootstrap
)
from arch.typing import ArrayLike, Int64Array
from numpy.random import Generator, RandomState
import numpy as np


def _get_random_integers(
    prng: Generator | RandomState, upper: int, *, size: int = 1
) -> Int64Array:  # type: ignore
    """From the arch package."""
    if isinstance(prng, Generator):
        return prng.integers(upper, size=size, dtype=np.int64)
    else:
        assert isinstance(prng, RandomState)
        return prng.randint(upper, size=size, dtype=np.int64)


class GumBootsBootstrap(IIDBootstrap):
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
