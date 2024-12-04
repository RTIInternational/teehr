import teehr.const as const
from teehr.evaluation.tables.base_table import BaseTable
from teehr.loading.locations import convert_locations
from teehr.models.filters import LocationFilter
from teehr.models.table_enums import LocationFields
from teehr.querying.utils import df_to_gdf
import teehr.models.pandera_dataframe_schemas as schemas
from pathlib import Path
from typing import Union
import logging
from teehr.utils.utils import to_path_or_s3path


logger = logging.getLogger(__name__)


class LocationTable(BaseTable):
    """Access methods to locations table."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.name = "locations"
        # self.dir = ev.locations_dir
        self.dir = to_path_or_s3path(ev.dataset_dir, self.name)
        self.format = "parquet"
        self.save_mode = "overwrite"
        self.filter_model = LocationFilter
        self.schema_func = schemas.locations_schema

    def field_enum(self) -> LocationFields:
        """Get the location fields enum."""
        fields = self._get_schema("pandas").columns.keys()
        return LocationFields(
            "LocationFields",
            {field: field for field in fields}
        )

    def to_pandas(self):
        """Return Pandas DataFrame for Location Table."""
        self._check_load_table()
        df = self.df.toPandas()
        df.attrs['table_type'] = 'locations'
        df.attrs['fields'] = self.fields()
        return df

    def to_geopandas(self):
        """Return GeoPandas DataFrame."""
        self._check_load_table()
        gdf = df_to_gdf(self.to_pandas())
        gdf.attrs['table_type'] = self.name
        gdf.attrs['fields'] = self.fields()
        return gdf

    def load_spatial(
        self,
        in_path: Union[Path, str],
        field_mapping: dict = None,
        pattern: str = "**/*.parquet",
        **kwargs
    ):
        """Import geometry data.

        Parameters
        ----------
        in_path : Union[Path, str]
            The input file or directory path.
            Any file format that can be read by GeoPandas.
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            Format: {input_field: output_field}
        pattern : str, optional (default: "**/*.parquet")
            The pattern to match files.
            Only used when in_path is a directory.
        **kwargs
            Additional keyword arguments are passed to GeoPandas read_file().

        File is first read by GeoPandas, field names renamed and
        then validated and inserted into the dataset.

        Notes
        -----

        The TEEHR Location Crosswalk table schema includes fields:

        - id
        - name
        - geometry
        """
        cache_dir = Path(
            self.ev.dir_path,
            const.CACHE_DIR,
            const.LOADING_CACHE_DIR,
            const.LOCATIONS_DIR
        )
        convert_locations(
            in_path,
            cache_dir,
            field_mapping=field_mapping,
            pattern=pattern,
            **kwargs
        )
        # Read the converted files to Spark DataFrame
        df = self._read_files(cache_dir)

        # Validate using the validate method
        validated_df = self._validate(df)

        # Write to the table
        self._write_spark_df(validated_df.repartition(1))

        # Reload the table
        self._load_table()