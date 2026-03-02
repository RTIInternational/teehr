"""LocationAttributesView - computed view for pivoted location attributes."""
from typing import List
import logging

from teehr.evaluation.views.base_view import View
import pyspark.sql as ps

logger = logging.getLogger(__name__)


class LocationAttributesView(View):
    """A computed view that pivots location attributes.

    Transforms the location_attributes table from long format
    (location_id, attribute_name, value) to wide format where each
    attribute becomes a column.

    Examples
    --------
    Pivot all attributes:

    >>> ev.pivoted_location_attributes_view().to_pandas()

    Pivot specific attributes:

    >>> ev.pivoted_location_attributes_view(
    ...     attr_list=["drainage_area", "ecoregion"]
    ... ).to_pandas()

    With filters (chained):

    >>> ev.pivoted_location_attributes_view().filter(
    ...     "location_id LIKE 'usgs%'"
    ... ).to_pandas()

    Materialize for later use:

    >>> ev.pivoted_location_attributes_view().write("pivoted_attrs")
    """

    def __init__(
        self,
        ev,
        attr_list: List[str] = None,
    ):
        """Initialize the LocationAttributesView.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance.
        attr_list : List[str], optional
            Specific attributes to include. If None, includes all.
        """
        super().__init__(ev)
        self._attr_list = attr_list

    def _compute(self) -> ps.DataFrame:
        """Compute the pivoted attributes.

        Returns
        -------
        ps.DataFrame
            The pivoted attributes DataFrame with one column per attribute.
        """
        logger.info("Computing pivoted attributes view")

        location_attributes_sdf = self._ev.location_attributes.to_sdf()

        if location_attributes_sdf.isEmpty():
            logger.warning("No location attributes found.")
            return self._ev.spark.createDataFrame(
                [], schema="location_id STRING"
            )

        # Determine which attributes to include
        if self._attr_list is not None:
            distinct_atts = list(set(self._attr_list))
            existing_atts = [
                row['attribute_name'] for row in
                location_attributes_sdf.select('attribute_name')
                .distinct().collect()
            ]
            valid_atts = [att for att in distinct_atts if att in existing_atts]

            # Filter to valid attributes
            location_attributes_sdf = location_attributes_sdf.filter(
                location_attributes_sdf.attribute_name.isin(valid_atts)
            )

            # Warn about missing attributes
            invalid_atts = set(distinct_atts) - set(valid_atts)
            if invalid_atts:
                logger.warning(
                    f"Attributes not found: {invalid_atts}. "
                    "They will not be included."
                )
        else:
            valid_atts = [
                row['attribute_name'] for row in
                location_attributes_sdf.select('attribute_name')
                .distinct().collect()
            ]

        # Pivot: location_id rows, attribute_name columns, value as cell value
        pivoted_df = (
            location_attributes_sdf
            .groupBy("location_id")
            .pivot("attribute_name", valid_atts)
            .agg({"value": "max"})
        )

        return pivoted_df
