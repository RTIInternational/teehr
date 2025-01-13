"""Initialize the TEEHR package."""
__version__ = "0.4.7"

from teehr.evaluation.evaluation import Evaluation  # noqa
from teehr.models.metrics.deterministic_models import DeterministicMetrics  # noqa
from teehr.models.metrics.probabilistic_models import ProbabilisticMetrics  # noqa
from teehr.models.metrics.signature_models import SignatureMetrics  # noqa
from teehr.models.metrics.bootstrap_models import Bootstrappers  # noqa
from teehr.models.metrics.basemodels import Operators  # noqa
from teehr.models.pydantic_table_models import (  # noqa
    Configuration,
    Attribute,
    Unit,
    Variable
)
from teehr.models.filters import (  # noqa
    UnitFilter,
    ConfigurationFilter,
    VariableFilter,
    AttributeFilter,
    LocationFilter,
    LocationAttributeFilter,
    LocationCrosswalkFilter,
    TimeseriesFilter,
    JoinedTimeseriesFilter
)

from teehr.models.calculated_fields.row_level import RowLevelCalculatedFields # noqa
from teehr.models.calculated_fields.timeseries_aware import TimeseriesAwareCalculatedFields # noqa

# For docs
from teehr.evaluation.tables.base_table import (  # noqa
    BaseTable,
)
from teehr.evaluation.tables.attribute_table import AttributeTable
from teehr.evaluation.tables.configuration_table import ConfigurationTable
from teehr.evaluation.tables.joined_timeseries_table import JoinedTimeseriesTable
from teehr.evaluation.tables.location_attribute_table import LocationAttributeTable
from teehr.evaluation.tables.location_crosswalk_table import LocationCrosswalkTable
from teehr.evaluation.tables.location_table import LocationTable
from teehr.evaluation.tables.primary_timeseries_table import PrimaryTimeseriesTable
from teehr.evaluation.tables.secondary_timeseries_table import SecondaryTimeseriesTable
from teehr.evaluation.tables.unit_table import UnitTable
from teehr.evaluation.tables.variable_table import VariableTable
from teehr.evaluation.fetch import Fetch  # noqa
from teehr.visualization.dataframe_accessor import TEEHRDataFrameAccessor  # noqa
from teehr.metrics import deterministic_funcs, signature_funcs, probabilistic_funcs # noqa
from teehr.metrics.signature_funcs import count  # noqa