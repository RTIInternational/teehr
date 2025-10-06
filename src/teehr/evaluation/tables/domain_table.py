"""Domain table class."""
from teehr.evaluation.tables.generic_table import Table
from teehr.models.pydantic_table_models import TableBaseModel
import pandas as pd
from typing import List, Union
import logging

logger = logging.getLogger(__name__)


class DomainTable(Table):
    """Domain table class."""

    def __init__(self, ev):
        """Initialize class."""
        super().__init__(ev)

    def _add(
        self,
        obj: Union[TableBaseModel, List[TableBaseModel]],
        write_mode: str = "append"
    ):
        # logger.info(f"Adding attribute to {self.dir}")
        self._check_load_table()

        org_df = self.to_sdf()

        if issubclass(type(obj), TableBaseModel):
            obj = [obj]

        # validate the data to be added
        new_df = self._ev.spark.createDataFrame(
            pd.DataFrame([o.model_dump() for o in obj])
            )
        logger.info(
            f"Validating {len(obj)} objects before adding to {self.table_name} table"
            )
        # if self.foreign_keys is not None:
        new_df_validated = self._ev.validate.data(
            df=new_df,
            table_schema=self.schema_func(),
        )

        # warn user if rows in added data already exist in the original table
        df_matched = new_df_validated.join(
            org_df, on=self.uniqueness_fields, how="left_semi"
        )
        if df_matched.count() == 0:
            # add the validated new data to the existing data
            logger.info(f"Adding {len(obj)} objects to {self.table_name} table")
            combined_df = org_df.unionByName(new_df_validated).repartition(1)

            # validate the combined data
            logger.info(
                f"Validating {self.table_name} table after adding {len(obj)} objects"
                )

            validated_df = self._ev.validate.data(
                df=combined_df,
                table_schema=self.schema_func(),
            )
            self._ev.write.to_warehouse(
                source_data=validated_df,
                table_name=self.table_name,
                write_mode=write_mode,
                uniqueness_fields=self.uniqueness_fields
            )
        else:
            # warn the user that some rows in the added data already exist
            matched_count = df_matched.count()
            logger.warning(
                f"{matched_count} rows in the added data already exist in the "
                f"{self.table_name} table. Skipping these duplicates. "
                )
            # Include a warning detailing which values are duplicates
            matched_values = df_matched.select(
                self.uniqueness_fields
                ).distinct()
            matched_values_list = matched_values.collect()
            matched_values_str = "; ".join(
                [
                    ", ".join(
                        f"{col}={row[col]}" for col in self.uniqueness_fields
                    )
                    for row in matched_values_list
                ]
            )
            logger.warning(
                f"Duplicate values in {self.uniqueness_fields}: "
                f"{matched_values_str}"
            )
            # add data that is not already in the original table
            new_df_not_matched = new_df_validated.join(
                org_df, on=self.uniqueness_fields, how="left_anti"
            )
            logger.info(
                f"Adding {new_df_not_matched.count()} new objects to "
                f"{self.table_name} table"
            )
            combined_df = org_df.unionByName(new_df_not_matched).repartition(1)
            # validate the combined data
            logger.info(
                f"Validating {self.table_name} table after adding "
                f"{new_df_not_matched.count()} new objects"
            )
            validated_df = self._ev.validate.data(
                df=combined_df,
                table_schema=self.schema_func(),
            )

            self._ev.write.to_warehouse(
                source_data=validated_df,
                table_name=self.table_name,
                write_mode=write_mode,
                uniqueness_fields=self.uniqueness_fields
            )
