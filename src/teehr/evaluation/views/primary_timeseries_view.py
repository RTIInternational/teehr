"""PrimaryTimeseriesView - computed view for primary timeseries with attrs."""
from typing import List
import logging

from teehr.evaluation.views.base_view import View
from teehr.evaluation.views.location_attributes_view import (
    LocationAttributesView
)
import pyspark.sql as ps

logger = logging.getLogger(__name__)


class PrimaryTimeseriesView(View):
    """A computed view of primary timeseries with optional location attributes.

    This view reads primary timeseries and optionally joins
    pivoted location attributes.

    Examples
    --------
    Basic usage:

    >>> ev.primary_timeseries_view().to_pandas()

    With filters (chained):

    >>> ev.primary_timeseries_view().filter(
    ...     "location_id LIKE 'usgs%'"
    ... ).to_pandas()

    With location attributes:

    >>> ev.primary_timeseries_view(
    ...     add_attrs=True,
    ...     attr_list=["drainage_area", "ecoregion"]
    ... ).to_pandas()

    Materialize:

    >>> ev.primary_timeseries_view(add_attrs=True).write("primary_with_attrs")
    """

    def __init__(
        self,
        ev,
        add_attrs: bool = False,
        attr_list: List[str] = None,
    ):
        """Initialize the PrimaryTimeseriesView.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance.
        add_attrs : bool, optional
            Whether to add location attributes. Default False.
        attr_list : List[str], optional
            Specific attributes to add. If None and add_attrs=True, adds all.
        """
        super().__init__(ev)
        self._add_attrs = add_attrs
        self._attr_list = attr_list

    def _compute(self) -> ps.DataFrame:
        """Compute the view.

        Returns
        -------
        ps.DataFrame
            The primary timeseries DataFrame, optionally with attributes.
        """
        logger.info("Computing primary timeseries view")

        result_df = self._ev.primary_timeseries.to_sdf()

        # Add attributes if requested
        if self._add_attrs:
            result_df = self._add_attributes(result_df)

        return result_df

    def _add_attributes(
        self,
        df: ps.DataFrame
    ) -> ps.DataFrame:
        """Add location attributes to the DataFrame.

        Uses LocationAttributesView to pivot attributes and join them.

        Parameters
        ----------
        df : ps.DataFrame
            The primary timeseries DataFrame.

        Returns
        -------
        ps.DataFrame
            The DataFrame with attributes added.
        """
        # Use LocationAttributesView to get pivoted attributes
        attrs_view = LocationAttributesView(
            ev=self._ev,
            attr_list=self._attr_list,
        )
        pivot_df = attrs_view.to_sdf()

        if pivot_df.isEmpty():
            logger.warning(
                "No location attributes found. Skipping adding attributes."
            )
            return df

        # Join pivoted attributes to primary timeseries on location_id
        df.createOrReplaceTempView("primary_ts")
        pivot_df.createOrReplaceTempView("attrs")

        # Get attr columns excluding location_id
        attr_cols = [c for c in pivot_df.columns if c != "location_id"]
        attr_select = ", ".join([f"attrs.{c}" for c in attr_cols])

        result_df = self._ev.spark.sql(f"""
            SELECT
                primary_ts.*,
                {attr_select}
            FROM primary_ts
            JOIN attrs
                ON primary_ts.location_id = attrs.location_id
        """)

        self._ev.spark.catalog.dropTempView("primary_ts")
        self._ev.spark.catalog.dropTempView("attrs")

        return result_df
