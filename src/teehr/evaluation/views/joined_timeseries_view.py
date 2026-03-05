"""JoinView - computed view for joining primary and secondary timeseries."""
from typing import List, Union
import logging

from teehr.evaluation.views.base_view import View
from teehr.models.filters import TableFilter
from teehr.evaluation.views.location_attributes_view import LocationAttributesView

import pyspark.sql as ps

logger = logging.getLogger(__name__)


class JoinedTimeseriesView(View):
    """A computed view that joins primary and secondary timeseries.

    This view performs the join operation between primary and secondary
    timeseries based on location crosswalks, value_time, unit_name, and
    variable name components. The computation is lazy - it only runs
    when the data is accessed.

    Examples
    --------
    Create different join views with different filters:

    >>> winter = ev.join_timeseries_view(primary_filters=["month IN (12, 1, 2)"])
    >>> summer = ev.join_timeseries_view(primary_filters=["month IN (6, 7, 8)"])

    Use the view directly (computes on-the-fly):

    >>> winter.to_pandas()

    Chain operations and materialize:

    >>> ev.join_timeseries_view().query(
    ...     include_metrics=[KGE()],
    ...     group_by=["primary_location_id"]
    ... ).write("location_kge")

    Materialize the joined data:

    >>> ev.join_timeseries_view(add_attrs=True).write("joined_timeseries")
    """

    def __init__(
        self,
        ev,
        primary_filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None,
        secondary_filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None,
        add_attrs: bool = False,
        attr_list: List[str] = None,
    ):
        """Initialize the JoinView.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance.
        primary_filters : Union[str, dict, TableFilter, List[...]], optional
            Filters to apply to primary timeseries before joining.
        secondary_filters : Union[str, dict, TableFilter, List[...]], optional
            Filters to apply to secondary timeseries before joining.
        add_attrs : bool, optional
            Whether to add location attributes to the result. Default False.
        attr_list : List[str], optional
            Specific attributes to add. If None and add_attrs=True, adds all.
        """
        super().__init__(ev)
        self._primary_filters = primary_filters
        self._secondary_filters = secondary_filters
        self._add_attrs = add_attrs
        self._attr_list = attr_list

    def _compute(self) -> ps.DataFrame:
        """Compute the join.

        Returns
        -------
        ps.DataFrame
            The joined timeseries DataFrame.
        """
        logger.info("Computing joined timeseries view")

        # Perform the join
        joined_df = self._perform_join()

        # Add attributes if requested
        if self._add_attrs:
            joined_df = self._add_attributes(joined_df)

        return joined_df

    def _perform_join(self) -> ps.DataFrame:
        """Perform the primary/secondary timeseries join.

        Joins primary and secondary timeseries based on location crosswalks,
        value_time, unit_name, and variable name components. Variable names
        are parsed into three parts: parameter, period, and statistic
        (e.g., "streamflow_hourly_inst" -> parameter="streamflow",
        period="hourly", statistic="inst").

        Join behavior based on variable name structure:
        - For 'inst' (instantaneous) statistics: Joins on parameter and
          statistic only, ignoring period.
        - For non-inst statistics: Requires parameter, period, AND statistic
          to match.

        Returns
        -------
        ps.DataFrame
            The joined DataFrame.
        """
        # Get filtered primary timeseries
        pt = self._ev.primary_timeseries
        if self._primary_filters is not None:
            pt = pt.filter(self._primary_filters)
        pt.to_sdf().createOrReplaceTempView("filtered_primary_timeseries")

        # Get filtered secondary timeseries
        st = self._ev.secondary_timeseries
        if self._secondary_filters is not None:
            st = st.filter(self._secondary_filters)
        st.to_sdf().createOrReplaceTempView("filtered_secondary_timeseries")

        # Execute the join query
        joined_df = self._ev.sql("""
            WITH exploded_variables AS (
                SELECT
                    name,
                    split(name, '_') as parts,
                    size(split(name, '_')) as num_parts
                FROM
                    variables
            ),
            variables_parsed AS (
                SELECT
                    name,
                    parts[0] as parameter,
                    CASE WHEN num_parts >= 2
                        THEN parts[1] ELSE NULL END as period,
                    CASE WHEN num_parts >= 3
                        THEN parts[2] ELSE NULL END as statistic
                FROM
                    exploded_variables
            ),
            primary AS (
                SELECT
                    pf.*,
                    v.parameter,
                    v.period,
                    v.statistic
                FROM
                    filtered_primary_timeseries pf
                JOIN variables_parsed v
                    ON v.name = pf.variable_name
            ),
            secondary AS (
                SELECT
                    sf.*,
                    v.parameter,
                    v.period,
                    v.statistic
                FROM
                    filtered_secondary_timeseries sf
                JOIN variables_parsed v
                    ON v.name = sf.variable_name
            )
            SELECT
                sf.reference_time
                , sf.value_time as value_time
                , pf.location_id as primary_location_id
                , sf.location_id as secondary_location_id
                , pf.value as primary_value
                , sf.value as secondary_value
                , sf.configuration_name
                , sf.unit_name
                , sf.variable_name
                , sf.member
            FROM secondary sf
            JOIN location_crosswalks cf
                ON cf.secondary_location_id = sf.location_id
            JOIN primary pf
                ON cf.primary_location_id = pf.location_id
                AND sf.value_time = pf.value_time
                AND sf.unit_name = pf.unit_name
                AND sf.parameter <=> pf.parameter
                AND sf.statistic <=> pf.statistic
                AND (sf.statistic = 'inst' OR sf.period <=> pf.period)
        """)

        # Clean up temp views
        self._ev.spark.catalog.dropTempView("filtered_primary_timeseries")
        self._ev.spark.catalog.dropTempView("filtered_secondary_timeseries")

        return joined_df

    def _add_attributes(
        self,
        joined_df: ps.DataFrame
    ) -> ps.DataFrame:
        """Add location attributes to the joined DataFrame.

        Uses LocationAttributesView to pivot attributes and join them.

        Parameters
        ----------
        joined_df : ps.DataFrame
            The joined timeseries DataFrame.

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
            return joined_df

        # Join pivoted attributes to the joined timeseries
        joined_df.createOrReplaceTempView("joined")
        pivot_df.createOrReplaceTempView("attrs")

        joined_df = self._ev.sql("""
            SELECT
                joined.*
                , attrs.*
            FROM joined
            JOIN attrs
                ON joined.primary_location_id = attrs.location_id
        """).drop("location_id")

        self._ev.spark.catalog.dropTempView("joined")
        self._ev.spark.catalog.dropTempView("attrs")

        return joined_df
