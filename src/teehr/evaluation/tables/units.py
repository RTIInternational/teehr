"""Access methods to units table."""
from typing import Union, List
from teehr.querying.filter_format import validate_and_apply_filters
from teehr.querying.utils import order_df
from teehr.models.dataset.table_models import (
    Unit
)
from teehr.models.dataset.table_enums import (
    UnitFields
)
from teehr.models.dataset.filters import (
    UnitFilter
)
import logging

logger = logging.getLogger(__name__)


class Units():
    """Access methods to units table."""

    def __init__(
        self,
        eval,
        filters: Union[UnitFilter, List[UnitFilter]] = None,
        order_by: Union[UnitFields, List[UnitFields]] = None
    ):
        """Initialize class."""
        self.units_dir = eval.units_dir
        self.spark = eval.spark
        self.df = (
            self.spark.read.format("csv")
            .option("recursiveFileLookup", "true")
            .option("mergeSchema", "true")
            .option("header", True)
            .load(str(self.units_dir))
        )
        if filters is not None:
            self.df = validate_and_apply_filters(self.df, filters, UnitFilter, Unit)
        if order_by is not None:
            self.df = order_df(self.df, order_by)

    def filter(self, filters):
        """Filter."""
        logger.info(f"Setting filter {filter}.")
        self.df = validate_and_apply_filters(self.df, filters, UnitFilter, Unit)
        return self

    def order_by(self, fields):
        """Orderby."""
        self.df = order_df(self.df, fields)
        return self

    def to_df(self):
        """Return Pandas DataFrame."""
        return self.df.toPandas()

    def to_sdf(self):
        """Return PySpark DataFrame."""
        return self.df
