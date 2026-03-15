"""SecondaryTimeseriesView - secondary timeseries with crosswalk and attrs."""
from typing import List, Union
import logging

from teehr.evaluation.views.base_view import View
import pyspark.sql as ps

logger = logging.getLogger(__name__)


class SecondaryTimeseriesView(View):
    """A computed view of secondary timeseries with crosswalk and attributes.

    This view joins secondary timeseries with location_crosswalks to add the
    primary_location_id, and optionally joins pivoted location attributes
    based on that primary_location_id.

    Examples
    --------
    Basic usage (adds primary_location_id via crosswalk):

    >>> ev.secondary_timeseries_view().to_pandas()

    With filters (chained):

    >>> ev.secondary_timeseries_view().filter(
    ...     "configuration_name = 'nwm30_retrospective'"
    ... ).to_pandas()

    With location attributes (joined on primary_location_id):

    >>> ev.secondary_timeseries_view(
    ...     add_attrs=True,
    ...     attr_list=["drainage_area", "ecoregion"]
    ... ).to_pandas()

    Materialize:

    >>> ev.secondary_timeseries_view(add_attrs=True).write("sec_with_attrs")
    """

    def __init__(
        self,
        ev,
        add_attrs: bool = False,
        attr_list: List[str] = None,
        catalog_name: Union[str, None] = None,
        namespace_name: Union[str, None] = None,
    ):
        """Initialize the SecondaryTimeseriesView.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance.
        add_attrs : bool, optional
            Whether to add location attributes. Default False.
        attr_list : List[str], optional
            Specific attributes to add. If None and add_attrs=True, adds all.
        catalog_name : Union[str, None], optional
            The catalog containing the source tables. If None, uses the
            active catalog.
        namespace_name : Union[str, None], optional
            The namespace containing the source tables. If None, uses the
            active catalog's namespace.
        """
        super().__init__(ev, catalog_name=catalog_name, namespace_name=namespace_name)
        self._add_attrs = add_attrs
        self._attr_list = attr_list

    def _compute(self) -> ps.DataFrame:
        """Compute the view.

        Joins secondary timeseries with location_crosswalks to add
        primary_location_id, and optionally adds location attributes.

        Returns
        -------
        ps.DataFrame
            The secondary timeseries DataFrame with primary_location_id
            and optionally location attributes.
        """
        logger.info("Computing secondary timeseries view")

        # Get secondary timeseries
        self._get_table("secondary_timeseries").to_sdf().createOrReplaceTempView(
            "secondary_ts"
        )

        # If a specific catalog/namespace is requested, create a temp view for
        # location_crosswalks so the SQL resolves it from the correct source
        _extra_views = []
        if self._catalog_name is not None or self._namespace_name is not None:
            self._get_table("location_crosswalks").to_sdf().createOrReplaceTempView(
                "location_crosswalks"
            )
            _extra_views.append("location_crosswalks")

        # Join secondary timeseries with crosswalks to add primary_location_id
        result_df = self._ev.sql("""
            SELECT
                st.*,
                cf.primary_location_id
            FROM secondary_ts st
            JOIN location_crosswalks cf
                ON cf.secondary_location_id = st.location_id
        """)

        self._ev.spark.catalog.dropTempView("secondary_ts")
        for view_name in _extra_views:
            self._ev.spark.catalog.dropTempView(view_name)

        # Add attributes if requested
        if self._add_attrs:
            result_df = self._add_attributes(result_df)

        return result_df

    def _add_attributes(
        self,
        df: ps.DataFrame
    ) -> ps.DataFrame:
        """Add location attributes to the DataFrame.

        Uses LocationAttributesView to pivot attributes and join them
        on the primary_location_id.

        Parameters
        ----------
        df : ps.DataFrame
            The secondary timeseries DataFrame with primary_location_id.

        Returns
        -------
        ps.DataFrame
            The DataFrame with attributes added.
        """
        # Use LocationAttributesView to get pivoted attributes
        attrs_df = self._ev.location_attributes_view(
            attr_list=self._attr_list,
            catalog_name=self._catalog_name,
            namespace_name=self._namespace_name,
        ).to_sdf()

        if attrs_df.isEmpty():
            logger.warning(
                "No location attributes found. Skipping adding attributes."
            )
            return df

        # Join pivoted attributes on primary_location_id
        df.createOrReplaceTempView("secondary_with_crosswalk")
        attrs_df.createOrReplaceTempView("attrs")

        # Get attr columns excluding location_id
        attr_cols = [c for c in attrs_df.columns if c != "location_id"]
        attr_select = ", ".join([f"attrs.{c}" for c in attr_cols])

        result_df = self._ev.sql(f"""
            SELECT
                st.*,
                {attr_select}
            FROM secondary_with_crosswalk st
            JOIN attrs
                ON st.primary_location_id = attrs.location_id
        """)

        self._ev.spark.catalog.dropTempView("secondary_with_crosswalk")
        self._ev.spark.catalog.dropTempView("attrs")

        return result_df
