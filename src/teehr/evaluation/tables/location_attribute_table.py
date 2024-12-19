import teehr.const as const
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.location_attributes import convert_location_attributes
from teehr.loading.utils import validate_input_is_csv, validate_input_is_parquet
from teehr.models.filters import LocationAttributeFilter
from teehr.models.table_enums import LocationAttributeFields
from teehr.querying.utils import join_geometry
import teehr.models.pandera_dataframe_schemas as schemas
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path


logger = logging.getLogger(__name__)


class LocationAttributeTable(BaseTable):
    """Access methods to location attributes table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "location_attributes"
        # self.dir = ev.location_attributes_dir
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.format = "parquet"
        self.save_mode = "overwrite"
        self.filter_model = LocationAttributeFilter
        self.schema_func = schemas.location_attributes_schema

    def _load(
        self,
        in_path: Union[Path, str],
        pattern: str = None,
        field_mapping: dict = None,
        **kwargs
    ):
        """Load location attributes helper."""
        cache_dir = Path(
            self.ev.dir_path,
            const.CACHE_DIR,
            const.LOADING_CACHE_DIR,
            const.LOCATION_ATTRIBUTES_DIR
        )
        convert_location_attributes(
            in_path,
            cache_dir,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        # Read the converted files to Spark DataFrame
        df = self._read_files(cache_dir)

        # Validate using the validate method
        validated_df = self._validate(df)

        # Write to the table df.rdd.getNumPartitions()
        self._write_spark_df(validated_df.repartition(df.rdd.getNumPartitions()))

        # Reload the table
        self._load_table()

    def field_enum(self) -> LocationAttributeFields:
        """Get the location attribute fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return LocationAttributeFields(
            "LocationAttributeFields",
            {field: field for field in fields}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Attributes."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = 'location_attributes'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = join_geometry(self.df, self.ev.locations.to_sdf())
        gdf.attrs['table_type'] = self.name
        gdf.attrs['fields'] = self.fields()
        return gdf

    def _get_schema(self, type: str = "pyspark"):
        """Get the location attribute schema."""
        if type == "pandas":
            return self.schema_func(type="pandas")

        location_ids = self.ev.locations.distinct_values("id")
        attr_names = self.ev.attributes.distinct_values("name")
        return self.schema_func(location_ids=location_ids, attr_names=attr_names)

    def load_parquet(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.parquet",
        field_mapping: dict = None,
        **kwargs
    ):
        """Import location_attributes from parquet file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Parquet file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Notes
        -----

        The TEEHR Location Attribute table schema includes fields:

        - location_id
        - attribute_name
        - value
        """
        validate_input_is_parquet(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        self._load_table()

    def load_csv(
        self,
        in_path: Union[Path, str],
        pattern: str = "**/*.csv",
        field_mapping: dict = None,
        **kwargs
    ):
        """Import location_attributes from CSV file format.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            CSV file format.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        **kwargs
            Additional keyword arguments are passed to pd.read_parquet().

        Notes
        -----

        The TEEHR Location Attribute table schema includes fields:

        - location_id
        - attribute_name
        - value
        """
        validate_input_is_csv(in_path)
        self._load(
            in_path=in_path,
            pattern=pattern,
            field_mapping=field_mapping,
            **kwargs
        )
        self._load_table()