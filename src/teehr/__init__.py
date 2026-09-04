"""Initialize the TEEHR package."""
import warnings

__version__ = "0.7.0"

with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    import pandera.pyspark as ps  # noqa: F401

from teehr.evaluation.evaluation import Evaluation  # noqa
from teehr.evaluation.evaluation import RemoteReadOnlyEvaluation  # noqa
from teehr.evaluation.evaluation import RemoteReadWriteEvaluation  # noqa
from teehr.evaluation.evaluation import LocalReadWriteEvaluation  # noqa
from teehr.metrics.models.deterministic import DeterministicMetrics  # noqa
from teehr.metrics.models.probabilistic import ProbabilisticMetrics  # noqa
from teehr.metrics.models.signature import Signatures  # noqa
from teehr.metrics.models.bootstrap import Bootstrappers  # noqa
from teehr.metrics.models.base import Operators  # noqa
from teehr.models.pydantic_table_models import (  # noqa
    Configuration,
    Attribute,
    Unit,
    Variable
)

from teehr.calculated_fields.models.row_level import RowLevelCalculatedFields # noqa
from teehr.calculated_fields.models.timeseries_aware import TimeseriesAwareCalculatedFields # noqa
from teehr.generate.models.timeseries_generator_models import (  # noqa
    SignatureTimeseriesGenerators,
    BenchmarkForecastGenerators
)
