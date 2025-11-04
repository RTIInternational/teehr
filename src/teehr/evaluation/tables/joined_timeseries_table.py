"""Joined Timeseries Table."""
import sys
from pathlib import Path
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.querying.utils import join_geometry
import pyspark.sql as ps
import logging
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel
from typing import List, Union

logger = logging.getLogger(__name__)


class JoinedTimeseriesTable(TimeseriesTable):
    """Access methods to joined timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)

    def __call__(
        self,
        table_name: str = "joined_timeseries",
        namespace_name: Union[str, None] = None,
        catalog_name: Union[str, None] = None,
    ):
        """Get an instance of the joined timeseries table.

        Note
        ----
        Creates an instance of a Table class with 'joined_timeseries'
        properties. If namespace_name or catalog_name are None, they are
        derived from the active catalog, which is 'local' by default.
        """
        return super().__call__(
            table_name=table_name,
            namespace_name=namespace_name,
            catalog_name=catalog_name
        )

    # TODO: Can't this be in the Table class?
    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        return join_geometry(
            self.sdf, self._ev.locations.to_sdf(),
            "primary_location_id"
        )

    def _join(self) -> ps.DataFrame:
        """Join primary and secondary timeseries."""
        joined_df = self._ev.sql("""
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
            FROM secondary_timeseries sf
            JOIN location_crosswalks cf
                on cf.secondary_location_id = sf.location_id
            JOIN primary_timeseries pf
                on cf.primary_location_id = pf.location_id
                and sf.value_time = pf.value_time
                and sf.unit_name = pf.unit_name
                and sf.variable_name = pf.variable_name
            """,
            create_temp_views=["secondary_timeseries",
                               "location_crosswalks",
                               "primary_timeseries"])
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

    def add_calculated_fields(self,
                              cfs: Union[CalculatedFieldBaseModel,
                                         List[CalculatedFieldBaseModel]]):
        """Add calculated fields to the joined timeseries table.

        Note this does not persist the CFs to the table. It only applies them
        to the DataFrame. To persist the CFs to the table, use the `write`
        method.

        Parameters
        ----------
        cfs : Union[CalculatedFieldBaseModel, List[CalculatedFieldBaseModel]]
            The CFs to apply to the DataFrame.

        Examples
        --------
        >>> import teehr
        >>> from teehr import RowLevelCalculatedFields as rcf
        >>> ev.join_timeseries.add_calculated_fields([
        >>>     rcf.Month()
        >>> ]).write()
        """
        self._check_load_table()

        if not isinstance(cfs, List):
            cfs = [cfs]

        for cf in cfs:
            self.sdf = cf.apply_to(self.sdf)

        return self

    def write(self, write_mode: str = "create_or_replace"):
        """Write the joined timeseries table to the warehouse."""
        # TODO: What should default write mode be?
        self._ev.write.to_warehouse(
            source_data=self.sdf,
            table_name=self.table_name,
            write_mode=write_mode,
            uniqueness_fields=self.uniqueness_fields,
        )
        logger.info("Joined timeseries table written to the warehouse.")
        self._load_table()

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
        write_mode: str = "create_or_replace"
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
        """
        joined_df = self._join()

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
