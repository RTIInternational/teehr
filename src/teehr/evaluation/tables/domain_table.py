from teehr.evaluation.tables.base_table import BaseTable
from teehr.models.pydantic_table_models import TableBaseModel
import pandas as pd
from typing import List, Union
import logging

logger = logging.getLogger(__name__)


class DomainTable(BaseTable):
    """Domain table class."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)
        self.format = "csv"

    def _add(
        self,
        obj: Union[TableBaseModel, List[TableBaseModel]]
    ):
        logger.info(f"Adding attribute to {self.dir}")
        self._check_load_table()

        org_df = self.to_sdf()

        if issubclass(type(obj), TableBaseModel):
            obj = [obj]

        # validate the data to be added
        new_df = self.spark.createDataFrame(
            pd.DataFrame([o.model_dump() for o in obj])
            )
        logger.info(
            f"Validating {len(obj)} objects before adding to {self.name} table"
            )
        new_df_validated = self._validate(new_df)

        # warn user if rows in added data already exist in the original table
        df_matched = new_df_validated.join(
            org_df, on=self.unique_column_set, how="left_semi"
        )
        if df_matched.count() == 0:
            # add the validated new data to the existing data
            logger.info(f"Adding {len(obj)} objects to {self.name} table")
            combined_df = org_df.unionByName(new_df_validated).repartition(1)

            # validate the combined data
            logger.info(
                f"Validating {self.name} table after adding {len(obj)} objects"
                )
            validated_df = self._validate(combined_df)

            # write the validated data to the table
            self._write_spark_df(validated_df)
        else:
            # warn the user that some rows in the added data already exist
            matched_count = df_matched.count()
            logger.warning(
                f"{matched_count} rows in the added data already exist in the "
                f"{self.name} table. Skipping these duplicates. "
                )
            # Include a warning detailing which values are duplicates
            matched_values = df_matched.select(
                self.unique_column_set
                ).distinct()
            matched_values_list = matched_values.collect()
            matched_values_str = "; ".join(
                [
                    ", ".join(
                        f"{col}={row[col]}" for col in self.unique_column_set
                    )
                    for row in matched_values_list
                ]
            )
            logger.warning(
                f"Duplicate values in {self.unique_column_set}: "
                f"{matched_values_str}"
            )

            # add data that is not already in the original table
            new_df_not_matched = new_df_validated.join(
                org_df, on=self.unique_column_set, how="left_anti"
            )
            logger.info(
                f"Adding {new_df_not_matched.count()} new objects to "
                f"{self.name} table"
            )
            combined_df = org_df.unionByName(new_df_not_matched).repartition(1)
            # validate the combined data
            logger.info(
                f"Validating {self.name} table after adding "
                f"{new_df_not_matched.count()} new objects"
            )
            validated_df = self._validate(combined_df)
            # write the validated data to the table
            self._write_spark_df(validated_df)
