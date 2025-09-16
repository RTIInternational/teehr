"""Class for extracting data from raw files."""
from pathlib import Path
from typing import List, Callable
import logging
import concurrent.futures

from pandera.pyspark import DataFrameSchema as SparkDataFrameSchema
from pandera import DataFrameSchema as PandasDataFrameSchema

from teehr.loading.utils import (
    merge_field_mappings,
    validate_constant_values_dict
)


logger = logging.getLogger(__name__)


class DataExtractor:
    """Class for extracting data from raw files."""

    def __init__(self, ev) -> None:
        """Initialize the Fetch class."""
        self.ev = ev

    @staticmethod
    def _merge_field_mapping(
        table_fields: List[str],
        field_mapping: dict = None,
        constant_field_values: dict = None
    ) -> dict:
        """Merge user field mapping with default field mapping.

        Parameters
        ----------
        table_fields : List[str]
            The list of table fields.
        field_mapping : dict, optional
            The user-provided field mapping.

        Returns
        -------
        dict
            The merged field mapping.
        """
        default_field_mapping = {}
        for field in table_fields:
            if field not in default_field_mapping.values():
                default_field_mapping[field] = field

        if field_mapping:
            logger.debug(
                "Merging user field_mapping with default field mapping."
            )
            field_mapping = merge_field_mappings(
                default_field_mapping,
                field_mapping
            )
        else:
            logger.debug("Using default field mapping.")
            field_mapping = default_field_mapping

        # verify constant_field_values keys are in field_mapping values
        if constant_field_values:
            validate_constant_values_dict(
                constant_field_values,
                field_mapping.values()
            )

        return field_mapping

    def to_cache(
        self,
        in_datapath: str | Path,
        cache_dir: str | Path,
        table_fields: list[str],
        table_schema_func: SparkDataFrameSchema | PandasDataFrameSchema,
        extraction_func: Callable[[str | Path], dict[str]],
        field_mapping: dict = None,
        constant_field_values: dict = None,
        pattern: str = "**/*.parquet",
        parallel: bool = False,
        max_workers: int = 1,
        **kwargs
    ):
        """Convert location attributes data to parquet format.

        Parameters
        ----------
        in_datapath : str | Path
            The input file or directory path.
        cache_dir : str | Path
            The directory to write the cached parquet files to.
        table_fields : list[str]
            The list of fields in the target table.
        table_schema_func : Callable
            A function that returns a pandera schema for validating the data.
        extraction_func : Callable
            A function that extracts a DataFrame from a raw data file.
            The function must have the signature:
            func(in_filepath: str | Path, field_mapping: dict, **kwargs) -> DataFrame
        field_mapping : dict, optional
            A dictionary mapping input fields to output fields.
            format: {input_field: output_field}
        constant_field_values : dict, optional
            A dictionary of constant field values to add to the DataFrame.
            format: {field: value}
        pattern : str, optional
            The glob pattern to use when searching for files in a directory.
            Default is '**/*.parquet' to search for all parquet files recursively.
        parallel : bool, optional
            Whether to process files in parallel. Default is False.
            Note: Parallel processing is not yet implemented.
        max_workers : int, optional
            The maximum number of worker processes to use if parallel is True.
            Default is 1. If set to -1, uses the number of CPUs available.
        **kwargs
            Additional keyword arguments are passed to
            the extraction function related to reading the raw data files.
        """  # noqa
        cache_dir = Path(cache_dir)
        in_datapath = Path(in_datapath)

        merged_field_mapping = self._merge_field_mapping(
            table_fields=table_fields,
            field_mapping=field_mapping,
            constant_field_values=constant_field_values
        )
        # Create cache directory if it doesn't exist
        cache_dir.mkdir(parents=True, exist_ok=True)

        files_converted = 0
        if in_datapath.is_dir() and parallel is False:
            logger.info(f"Converting all files in {in_datapath}/{pattern}")
            for in_filepath in in_datapath.glob(f"{pattern}"):
                relative_name = in_filepath.relative_to(in_datapath)
                out_filepath = Path(cache_dir, relative_name)
                out_filepath = out_filepath.with_suffix(".parquet")
                # Extract dataframe
                df = extraction_func(
                    in_filepath=in_filepath,
                    field_mapping=merged_field_mapping,
                    **kwargs
                )
                # Apply constant field values
                if constant_field_values:
                    for field, value in constant_field_values.items():
                        df[field] = value
                # Validate data types and required fields
                validated_df = table_schema_func.validate(df)
                # Write to cache as parquet
                self.ev.write.to_cache(
                    validated_df,
                    out_filepath
                )
                files_converted += 1
        elif in_datapath.is_dir() and parallel is True:
            filepaths = sorted(list(in_datapath.glob(f"{pattern}")))
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=max_workers
            ) as executor:
                futures = {}
                for in_filepath in filepaths:
                    future = executor.submit(
                        extraction_func,
                        in_filepath,
                        merged_field_mapping,
                        **kwargs
                    )
                    futures[future] = in_filepath.name
                for future in concurrent.futures.as_completed(futures):
                    df = future.result()
                    filename = futures[future]
                    out_filepath = Path(cache_dir, filename)
                    out_filepath = out_filepath.with_suffix(".parquet")
                    # Apply constant field values
                    if constant_field_values:
                        for field, value in constant_field_values.items():
                            df[field] = value
                    # Validate data types and required fields
                    validated_df = table_schema_func.validate(df)
                    # Write to cache as parquet
                    self.ev.write.to_cache(
                        validated_df,
                        out_filepath
                    )
                    files_converted += 1
        else:
            out_filepath = Path(cache_dir, in_datapath.name)
            out_filepath = out_filepath.with_suffix(".parquet")
            # Extract dataframe
            df = extraction_func(
                in_filepath=in_datapath,
                field_mapping=merged_field_mapping,
                **kwargs
            )
            # Apply constant field values
            if constant_field_values:
                for field, value in constant_field_values.items():
                    df[field] = value
            # Validate data types and required fields
            validated_df = table_schema_func.validate(df)
            # Write to cache as parquet
            self.ev.write.to_cache(
                validated_df,
                out_filepath
            )
            files_converted += 1
        logger.info(f"Converted {files_converted} files.")
