"""Initialize the TEEHR package."""
import warnings

__version__ = "0.5.2"

with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    import pandera.pyspark as ps  # noqa: F401

from teehr.evaluation.evaluation import Evaluation  # noqa
from teehr.models.metrics.deterministic_models import DeterministicMetrics  # noqa
from teehr.models.metrics.probabilistic_models import ProbabilisticMetrics  # noqa
from teehr.models.metrics.signature_models import Signatures  # noqa
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
from teehr.models.generate.timeseries_generator_models import (  # noqa
    SignatureTimeseriesGenerators,
    BenchmarkForecastGenerators
)
