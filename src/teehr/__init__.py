"""Initialize the TEEHR package."""
import warnings

__version__ = "0.6.2"

with warnings.catch_warnings():
    warnings.simplefilter("ignore", UserWarning)
    import pandera.pyspark as ps  # noqa: F401

from teehr.evaluation.evaluation import Evaluation  # noqa
from teehr.evaluation.evaluation import RemoteReadOnlyEvaluation  # noqa
from teehr.evaluation.evaluation import RemoteReadWriteEvaluation  # noqa
from teehr.evaluation.evaluation import LocalReadWriteEvaluation  # noqa
from teehr.metrics.deterministic_models import DeterministicMetrics  # noqa
from teehr.metrics.probabilistic_models import ProbabilisticMetrics  # noqa
from teehr.metrics.signature_models import Signatures  # noqa
from teehr.metrics.bootstrap_models import Bootstrappers  # noqa
from teehr.metrics.base_models import Operators  # noqa
from teehr.models.pydantic_table_models import (  # noqa
    Configuration,
    Attribute,
    Unit,
    Variable
)

from teehr.calculated_fields.row_level_models import RowLevelCalculatedFields # noqa
from teehr.calculated_fields.timeseries_aware_models import TimeseriesAwareCalculatedFields # noqa
from teehr.models.generate.timeseries_generator_models import (  # noqa
    SignatureTimeseriesGenerators,
    BenchmarkForecastGenerators
)
