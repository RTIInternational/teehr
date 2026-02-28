"""Joined Timeseries Table."""
import sys
from pathlib import Path
from teehr.evaluation.tables.base_table import BaseTable
from teehr.models.filters import TableFilter
from teehr.querying.utils import join_geometry
import pyspark.sql as ps
import logging
from typing import List, Union

logger = logging.getLogger(__name__)


class JoinedTimeseriesTable(BaseTable):
    """Access methods to joined timeseries table."""

    def __init__(
        self,
        ev,
        table_name: str = "joined_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Initialize the Table class.

        Parameters
        ----------
        ev : EvaluationBase
            The parent Evaluation instance providing access to Spark session,
            catalogs, and related table operations.
        table_name : str, optional
            The name of the table to operate on. Defaults to 'joined_timeseries'.
        namespace_name : Union[str, None], optional
            The namespace containing the table. If None, uses the
            active catalog's namespace.
        catalog_name : Union[str, None], optional
            The catalog containing the table. If None, uses the
            active catalog name.
        """
        super().__init__(ev, table_name, namespace_name, catalog_name)

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = join_geometry(
            self.sdf, self._ev.locations.to_sdf(),
            "primary_location_id"
        )
        gdf.attrs['table_type'] = self.table_name
        gdf.attrs['fields'] = self.to_sdf().columns
        return gdf

    def _join(self,
            primary_filters: Union[
                str, dict, TableFilter,
                List[Union[str, dict, TableFilter]]
            ] = None,
            secondary_filters: Union[
                str, dict, TableFilter,
                List[Union[str, dict, TableFilter]]
            ] = None,
        ) -> ps.DataFrame:
        """Join primary and secondary timeseries.

        Parameters
        ----------
        primary_filters : Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]], optional
            Filters to apply to the primary timeseries before joining,
            by default None
        secondary_filters : Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]], optional
            Filters to apply to the secondary timeseries before joining,
            by default None"""

        self._ev.primary_timeseries.filter(primary_filters).to_sdf().createOrReplaceTempView("filtered_primary_timeseries")

        self._ev.secondary_timeseries.filter(secondary_filters).to_sdf().createOrReplaceTempView("filtered_secondary_timeseries")

        self._ev.location_crosswalks.to_sdf().createOrReplaceTempView("location_crosswalks")

        joined_df = self._ev.spark.sql("""
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
            FROM filtered_secondary_timeseries sf
            JOIN location_crosswalks cf
                on cf.secondary_location_id = sf.location_id
            JOIN filtered_primary_timeseries pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.unit_name = pf.unit_name
                and sf.variable_name = pf.variable_name
            """)

        self._ev.spark.catalog.dropTempView("filtered_primary_timeseries")
        self._ev.spark.catalog.dropTempView("filtered_secondary_timeseries")
        self._ev.spark.catalog.dropTempView("location_crosswalks")

        return joined_df

    def _add_attr(self,
                  joined_df: ps.DataFrame,
                  attr_list: List[str] = None) -> ps.DataFrame:
        """Add attributes to the joined timeseries dataframe."""
        location_attributes_sdf = self._ev.location_attributes.to_sdf()
        if location_attributes_sdf.isEmpty():
            logger.warning(
                "No location attributes found. Skipping adding attributes to "
                "joined timeseries.")
            return joined_df

        joined_df.createTempView("joined")

        if attr_list is not None:
            # get unique attributes
            distinct_atts = list(set(attr_list))
            existing_atts = [
                row['attribute_name'] for row in
                location_attributes_sdf.select('attribute_name')
                .distinct().collect()
            ]
            # filter distinct to those that exist in the attribute table
            valid_atts = [att for att in distinct_atts if att in existing_atts]
            # filter input dataframe to just valid attributes
            location_attributes_sdf = location_attributes_sdf.filter(
                location_attributes_sdf.attribute_name.isin(valid_atts)
            )
            # warn users if any attributes in attr_list are not found
            invalid_atts = set(distinct_atts) - set(valid_atts)
            if invalid_atts:
                logger.warning(
                    "The following attributes were not found in the location "
                    f"attributes table: {invalid_atts}. "
                    "They will not be added to the joined timeseries table."
                )
        else:
            logger.info("No attribute list provided. Adding all attributes.")
            valid_atts = self._ev.location_attributes.distinct_values(
                "attribute_name")

        # Pivot the table
        pivot_df = (
            location_attributes_sdf.groupBy("location_id")
            .pivot("attribute_name", valid_atts).agg({"value": "max"})
        )

        # Create a view
        pivot_df.createTempView("attrs")

        joined_df = self._ev.spark.sql("""
            SELECT
                joined.*
                , attrs.*
            FROM joined
            JOIN attrs
                on joined.primary_location_id = attrs.location_id
        """).drop("location_id")

        self._ev.spark.catalog.dropTempView("joined")
        self._ev.spark.catalog.dropTempView("attrs")

        return joined_df

    def _run_script(self, joined_df: ps.DataFrame) -> ps.DataFrame:
        """Add UDFs to the joined timeseries dataframe."""
        try:
            sys.path.append(str(Path(self._ev.scripts_dir).resolve()))
            import user_defined_fields as udf # noqa
            joined_df = udf.add_user_defined_fields(joined_df)
        except ImportError:
            logger.info(
                f"No user-defined fields found in {self._ev.scripts_dir}."
                "Not adding user-defined fields."
            )
            return joined_df

        return joined_df

    def create(
        self,
        add_attrs: bool = True,
        execute_scripts: bool = False,
        attr_list: List[str] = None,
        write_mode: str = "create_or_replace",
        primary_filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None,
        secondary_filters: Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]
        ] = None,
    ):
        """Create joined timeseries table.

        Parameters
        ----------
        execute_scripts : bool, optional
            Execute UDFs, by default False
        add_attrs : bool, optional
            Add location attributes, by default True
        attr_list : List[str], optional
            List of attributes to add to the joined timeseries table.
            If None, all attributes are added. The default is None.
            add_attrs must be True for this to work.
        write_mode : str, optional
            Write mode for the joined timeseries table, by default
            "create_or_replace"
        primary_filters : Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]], optional
            Filters to apply to the primary timeseries before joining,
            by default None
        secondary_filters : Union[
            str, dict, TableFilter,
            List[Union[str, dict, TableFilter]]], optional
            Filters to apply to the secondary timeseries before joining,
            by default None
        """
        joined_df = self._join(primary_filters, secondary_filters)

        if add_attrs:
            joined_df = self._add_attr(joined_df, attr_list)

        if execute_scripts:
            joined_df = self._run_script(joined_df)

        validated_df = self._ev.validate.data(
            df=joined_df,
            table_schema=self.schema_func()
        )
        self._ev.write.to_warehouse(
            source_data=validated_df,
            table_name=self.table_name,
            write_mode=write_mode
        )
        logger.info("Joined timeseries table created.")
        self._load_table()

        return self


    def write(
        self,
        table_name: str = "joined_timeseries",
        write_mode: str = "create_or_replace"
    ):
        """Write the DataFrame to a warehouse table.

        Parameters
        ----------
        table_name : str
            The name of the table to write to.
        write_mode : str, optional
            The write mode to use, by default "create_or_replace"
            Options are: "create", "append", "overwrite", "create_or_replace"

        Example
        -------
        >>> import teehr
        >>> ev = teehr.Evaluation()
        Calculate some metrics and write to the warehouse.
        >>> metrics_df = ev.metrics.query(
        >>>     include_metrics=[...],
        >>>     group_by=["primary_location_id"]
        >>> ).write_to_warehouse(
        >>>     table_name="metrics",
        >>>     write_mode="create_or_replace"
        >>> )
        """
        logger.info(
            f"Writing metrics results to the warehouse table: {table_name}."
        )
        self._check_load_table()
        self._write.to_warehouse(
            source_data=self.sdf,
            table_name=table_name,
            write_mode=write_mode
        )
        return self