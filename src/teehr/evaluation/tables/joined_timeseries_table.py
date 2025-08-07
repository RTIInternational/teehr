import sys
from pathlib import Path
from teehr.evaluation.tables.timeseries_table import TimeseriesTable
from teehr.models.filters import JoinedTimeseriesFilter
from teehr.models.table_enums import JoinedTimeseriesFields
from teehr.querying.utils import join_geometry
import teehr.models.pandera_dataframe_schemas as schemas
import pyspark.sql as ps
import logging
from teehr.models.calculated_fields.base import CalculatedFieldBaseModel
from typing import List, Union
from teehr.utils.s3path import S3Path
from teehr.utils.utils import to_path_or_s3path, path_to_spark

logger = logging.getLogger(__name__)


class JoinedTimeseriesTable(TimeseriesTable):
    """Access methods to joined timeseries table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "joined_timeseries"
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.filter_model = JoinedTimeseriesFilter
        self.validate_filter_field_types = False
        self.strict_validation = False
        self.schema_func = schemas.joined_timeseries_schema
        self.unique_column_set = [
            "primary_location_id",
            "secondary_location_id",
            "value_time",
            "reference_time",
            "variable_name",
            "unit_name",
            "configuration_name",
        ]

    def field_enum(self) -> JoinedTimeseriesFields:
        """Get the joined timeseries fields enum."""
        self._check_load_table()
        fields_list = self.df.columns
        return JoinedTimeseriesFields(
            "JoinedTimeseriesFields",
            {field: field for field in fields_list}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Joined Timeseries."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = 'joined_timeseries'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        return join_geometry(
            self.df, self.ev.locations.to_sdf(),
            "primary_location_id"
        )

    def _join(self) -> ps.DataFrame:
        """Join primary and secondary timeseries."""
        joined_df = self.ev.sql("""
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
        location_attributes_sdf = self.ev.location_attributes.to_sdf()
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
            valid_atts = self.ev.location_attributes.distinct_values(
                "attribute_name")

        # Pivot the table
        pivot_df = (
            location_attributes_sdf.groupBy("location_id")
            .pivot("attribute_name", valid_atts).agg({"value": "max"})
        )

        # Create a view
        pivot_df.createTempView("attrs")

        joined_df = self.spark.sql("""
            SELECT
                joined.*
                , attrs.*
            FROM joined
            JOIN attrs
                on joined.primary_location_id = attrs.location_id
        """).drop("location_id")

        self.spark.catalog.dropTempView("joined")
        self.spark.catalog.dropTempView("attrs")

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
            self.df = cf.apply_to(self.df)

        return self

    def write(self, drop_duplicates: bool = False):
        """Write the joined timeseries table to disk."""
        # Validate to fix 'Cannot use NullType for partition column' error.
        validated_df = self._validate(
            df=self.df,
            strict=False,
            drop_duplicates=drop_duplicates
        )
        self._write_spark_df(
            validated_df,
            write_mode="overwrite",
        )
        logger.info("Joined timeseries table written to disk.")
        self._load_table()

    def _run_script(self, joined_df: ps.DataFrame) -> ps.DataFrame:
        """Add UDFs to the joined timeseries dataframe."""
        try:
            sys.path.append(str(Path(self.ev.scripts_dir).resolve()))
            import user_defined_fields as udf # noqa
            joined_df = udf.add_user_defined_fields(joined_df)
        except ImportError:
            logger.info(
                f"No user-defined fields found in {self.ev.scripts_dir}."
                "Not adding user-defined fields."
            )
            return joined_df

        return joined_df

    def create(
        self,
        add_attrs: bool = True,
        execute_scripts: bool = False,
        drop_duplicates: bool = False,
        attr_list: List[str] = None
    ):
        """Create joined timeseries table.

        Parameters
        ----------
        execute_scripts : bool, optional
            Execute UDFs, by default False
        add_attrs : bool, optional
            Add location attributes, by default True
        drop_duplicates : bool, optional
            Drop duplicates from the joined timeseries table, by default False.
            If duplicates exist, the first occurence is retained.
        attr_list : List[str], optional
            List of attributes to add to the joined timeseries table.
            If None, all attributes are added. The default is None.
            add_attrs must be True for this to work.
        """
        joined_df = self._join()

        if add_attrs:
            joined_df = self._add_attr(joined_df, attr_list)

        if execute_scripts:
            joined_df = self._run_script(joined_df)

        validated_df = self._validate(
            df=joined_df,
            strict=False,
            drop_duplicates=drop_duplicates
        )
        self._write_spark_df(
            validated_df,
            write_mode="overwrite",
        )
        logger.info("Joined timeseries table created.")
        self._load_table()

    def _read_files(
        self,
        path: Union[str, Path, S3Path],
        pattern: str = None,
        show_missing_table_warning: bool = False,
        **options
    ) -> ps.DataFrame:
        """Read data from table directory as a spark dataframe.

        Parameters
        ----------
        path : Union[str, Path, S3Path]
            The path to the directory containing the files.
        pattern : str, optional
            The pattern to match files.
        show_missing_table_warning : bool, optional
            If True, show the warning an empty table was returned.
            The default is True.
        **options
            Additional options to pass to the spark read method.

        Returns
        -------
        df : ps.DataFrame
            The spark dataframe.
        """
        logger.info(f"Reading files from {path}.")
        if len(options) == 0:
            options = {
                "header": "true",
                "ignoreMissingFiles": "true"
            }

        path = to_path_or_s3path(path)

        path = path_to_spark(path, pattern)

        # First, read the file with the schema and check if it's empty.
        # If it's not empty and it's the joined timeseries table,
        # read it again without the schema to ensure all fields are included.
        # Otherwise, continue.
        schema = self.schema_func().to_structtype()
        df = self.ev.spark.read.format(self.format).options(**options).load(
            path, schema=schema)
        if df.isEmpty():
            if show_missing_table_warning:
                logger.warning(
                    f"An empty dataframe was returned for '{self.name}'."
                    )
        elif not df.isEmpty():
            df = self.ev.spark.read.format(self.format).options(**options).load(path)

        return df
