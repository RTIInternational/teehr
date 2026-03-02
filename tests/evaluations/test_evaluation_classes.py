"""Test the subclasses of the Evaluation class."""
import pytest

from teehr.evaluation.evaluation import (
    RemoteReadOnlyEvaluation, RemoteReadWriteEvaluation
)


def test_read_only_evaluation(spark_shared_session, tmpdir):
    """Test the read only evaluation."""
    # This should only work in TEEHR-Hub
    with pytest.raises(ValueError):
        _ = RemoteReadOnlyEvaluation(
            spark=spark_shared_session,
            temp_dir_path=tmpdir
        )


def test_read_write_evaluation(spark_shared_session, tmpdir):
    """Test the read write evaluation."""
    # This should only work in TEEHR-Hub
    with pytest.raises(ValueError):
        _ = RemoteReadWriteEvaluation(
            spark=spark_shared_session,
            temp_dir_path=tmpdir
        )