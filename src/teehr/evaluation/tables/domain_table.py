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
        self.save_mode = "overwrite"

    def _add(
        self,
        obj: Union[TableBaseModel, List[TableBaseModel]]
    ):
        logger.info(f"Adding attribute to {self.dir}")
        self._check_load_table()

        org_df = self.to_sdf()

        if issubclass(type(obj), TableBaseModel):
            obj = [obj]

        new_df = self.spark.createDataFrame(pd.DataFrame([o.model_dump() for o in obj]))

        combined_df = org_df.unionByName(new_df).repartition(1)
        validated_df = self._validate(combined_df)

        self._write_spark_df(validated_df)