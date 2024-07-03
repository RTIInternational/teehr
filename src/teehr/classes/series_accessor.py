"""Provides the teehr accessor extending pandas Series objects."""
import pandas as pd
import numpy as np


@pd.api.extensions.register_series_accessor("teehr")
class SeriesAccessor:
    """Extends pandas Series objects. MAYBE UNNECESSARY."""

    def __init__(self, pandas_obj):
        """Initialize the class."""
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        # Validate dtype of the series
        if obj.dtype not in [
            np.dtype("object"),
            np.dtype("float64"),
            np.dtype("int64"),
            np.dtype("datetime64[us]"),
            np.dtype("timedelta64[ns]"),
        ]:
            raise AttributeError("Invalid series dtype.")
        pass

    # @property
    # def center(self):
    #     """Some property."""
    #     # return the geographic center point of this DataFrame
    #     lat = self._obj.latitude
    #     lon = self._obj.longitude
    #     return (float(lon.mean()), float(lat.mean()))

    def plot(self):
        """Plot something."""
        print("About to plot some Series data up in here.")
        pass
