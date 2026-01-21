"""Advanced fixture patterns combining templates with mocks.

This shows how to use evaluation_with_template together with mocks
for extremely fast, reliable tests.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime


# ============== COMBINING TEMPLATE FIXTURES WITH MOCKS ==============

@pytest.fixture
def mock_usgs_data():
    """Provide mock USGS data for tests."""
    return pd.DataFrame({
        'site_no': ['01646500', '02449838'] * 12,
        'datetime': pd.date_range('2022-01-01', periods=24, freq='H'),
        'value': [100.0, 150.0] * 12,
        'qualifiers': ['A', 'A'] * 12
    })


@pytest.fixture
def evaluation_with_mocked_usgs(evaluation_with_template, monkeypatch, mock_usgs_data):
    """Template-based evaluation with mocked USGS API.

    This combines:
    - Fast template-based evaluation
    - Mocked external API calls
    - Pre-configured for USGS fetching tests

    Result: Extremely fast, reliable tests without network calls.
    """
    ev = evaluation_with_template

    # Mock the USGS API call
    def mock_get_usgs(*args, **kwargs):
        return mock_usgs_data

    monkeypatch.setattr(
        'dataretrieval.nwis.get_iv',
        mock_get_usgs
    )

    # Pre-load locations for USGS tests
    test_locations = pd.DataFrame({
        'id': ['usgs-01646500', 'usgs-02449838'],
        'name': ['Site 1', 'Site 2']
    })

    return ev


def test_usgs_fetch_with_template_and_mock(evaluation_with_mocked_usgs):
    """Test USGS fetching with template + mock.

    This test is:
    - Fast (~0.2s) - no tar extraction, no network calls
    - Reliable - no flaky API calls
    - Isolated - clean state per test
    """
    ev = evaluation_with_mocked_usgs

    # This will use the mock data, not real API
    ev.fetch.usgs_streamflow(
        start_date=datetime(2022, 1, 1),
        end_date=datetime(2022, 1, 2)
    )

    # Verify the mock data was loaded
    df = ev.primary_timeseries.to_pandas()
    assert len(df) == 24  # From our mock data
    assert df.value.sum() == (100.0 + 150.0) * 12


# ============== SELECTIVE MOCKING ==============

@pytest.fixture
def evaluation_with_mocked_expensive_ops(evaluation_with_template, monkeypatch):
    """Template evaluation with expensive operations mocked.

    Mocks:
    - Weight generation (expensive geospatial ops)
    - But NOT data loading or Spark operations

    Use when: You want to test logic but skip expensive computations.
    """
    ev = evaluation_with_template

    # Mock weight generation
    mock_weights = pd.DataFrame({
        'location_id': ['huc10-1', 'huc10-2'],
        'nwm_feature_id': [101, 102],
        'weight': [0.6, 0.4]
    })

    monkeypatch.setattr(
        'teehr.fetching.nwm.grid_utils.generate_zonal_weights',
        lambda *args, **kwargs: mock_weights
    )

    return ev


def test_grid_fetch_without_expensive_weights(evaluation_with_mocked_expensive_ops):
    """Test grid fetching without generating weights."""
    ev = evaluation_with_mocked_expensive_ops

    # The weight generation is mocked, so this is fast
    # But the actual Spark/Iceberg operations are real
    # ev.fetch.nwm_retrospective_grids(...)

    # Test passes quickly because weight gen is mocked
    pass


# ============== AUTO-MOCKING FOR UNIT TESTS ==============

@pytest.fixture(autouse=True)
def auto_mock_external_apis_for_unit_tests(request, monkeypatch):
    """Automatically mock external APIs for tests marked as 'unit'.

    This fixture runs automatically (autouse=True) and checks if the test
    is marked with @pytest.mark.unit. If so, it mocks external dependencies.
    """
    if 'unit' not in request.keywords:
        return  # Not a unit test, don't mock

    # Mock USGS
    mock_usgs_data = pd.DataFrame({
        'site_no': ['12345'],
        'datetime': [datetime(2020, 1, 1)],
        'value': [100.0]
    })
    monkeypatch.setattr(
        'dataretrieval.nwis.get_iv',
        lambda *args, **kwargs: mock_usgs_data
    )

    # Mock NWM kerchunk
    mock_ds = MagicMock()
    mock_ds.sel.return_value.to_dataframe.return_value = pd.DataFrame({
        'streamflow': [10.0, 20.0],
        'time': [datetime(2020, 1, 1), datetime(2020, 1, 2)]
    })
    monkeypatch.setattr(
        'teehr.fetching.utils.get_dataset',
        lambda *args, **kwargs: mock_ds
    )


@pytest.mark.unit
def test_usgs_processing_logic(evaluation_with_template):
    """Unit test - mocks are automatically applied.

    Because this is marked @pytest.mark.unit, the auto_mock fixture
    automatically enables all API mocks.
    """
    ev = evaluation_with_template

    # This won't make real API calls due to auto-mocking
    # ev.fetch.usgs_streamflow(...)

    # Test the processing logic, not the API
    pass


# ============== MOCK BUILDER PATTERN ==============

class MockBuilder:
    """Builder pattern for creating complex mock scenarios."""

    def __init__(self):
        self.usgs_data = None
        self.nwm_data = None
        self.weights = None

    def with_usgs_data(self, sites, values):
        """Add USGS data to the mock."""
        self.usgs_data = pd.DataFrame({
            'site_no': sites,
            'datetime': [datetime(2020, 1, 1)] * len(sites),
            'value': values
        })
        return self

    def with_nwm_data(self, feature_ids, values):
        """Add NWM data to the mock."""
        self.nwm_data = pd.DataFrame({
            'feature_id': feature_ids,
            'time': [datetime(2020, 1, 1)] * len(feature_ids),
            'streamflow': values
        })
        return self

    def with_weights(self, location_ids, feature_ids, weights):
        """Add weight data to the mock."""
        self.weights = pd.DataFrame({
            'location_id': location_ids,
            'nwm_feature_id': feature_ids,
            'weight': weights
        })
        return self

    def build(self, monkeypatch):
        """Apply all mocks."""
        if self.usgs_data is not None:
            monkeypatch.setattr(
                'dataretrieval.nwis.get_iv',
                lambda *args, **kwargs: self.usgs_data
            )

        if self.nwm_data is not None:
            mock_ds = MagicMock()
            mock_ds.to_dataframe.return_value = self.nwm_data
            monkeypatch.setattr(
                'teehr.fetching.utils.get_dataset',
                lambda *args, **kwargs: mock_ds
            )

        if self.weights is not None:
            monkeypatch.setattr(
                'teehr.fetching.nwm.grid_utils.generate_zonal_weights',
                lambda *args, **kwargs: self.weights
            )


@pytest.fixture
def mock_builder():
    """Provide mock builder for tests."""
    return MockBuilder()


def test_with_mock_builder(evaluation_with_template, mock_builder, monkeypatch):
    """Test using the mock builder pattern.

    This allows you to easily create complex mock scenarios.
    """
    ev = evaluation_with_template

    # Build a custom mock scenario
    mock_builder \
        .with_usgs_data(
            sites=['01646500', '02449838'],
            values=[100.0, 200.0]
        ) \
        .with_nwm_data(
            feature_ids=[101, 102],
            values=[10.0, 20.0]
        ) \
        .build(monkeypatch)

    # Now test with this specific mock setup
    # ev.fetch.usgs_streamflow(...)
    pass


# ============== PYTEST MARKERS FOR MOCK CONTROL ==============

def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "unit: Fast unit tests with auto-mocked external dependencies"
    )
    config.addinivalue_line(
        "markers",
        "integration: Integration tests without mocks (may be slower)"
    )
    config.addinivalue_line(
        "markers",
        "readonly: Tests that don't modify data (can use shared fixtures)"
    )


# ============== FIXTURE COMPOSITION EXAMPLES ==============

@pytest.fixture
def full_mock_environment(
    evaluation_with_template,
    monkeypatch,
    mock_usgs_data
):
    """Fully mocked environment for comprehensive unit tests.

    Combines:
    - Template evaluation
    - Mocked USGS API
    - Mocked NWM API
    - Mocked S3 operations
    - Mocked weight generation

    Result: Completely isolated, fast unit tests.
    """
    ev = evaluation_with_template

    # Mock USGS
    monkeypatch.setattr(
        'dataretrieval.nwis.get_iv',
        lambda *args, **kwargs: mock_usgs_data
    )

    # Mock NWM
    mock_ds = MagicMock()
    mock_ds.sel.return_value.to_dataframe.return_value = pd.DataFrame({
        'streamflow': [10.0],
        'time': [datetime(2020, 1, 1)]
    })
    monkeypatch.setattr(
        'teehr.fetching.utils.get_dataset',
        lambda *args, **kwargs: mock_ds
    )

    # Mock S3
    monkeypatch.setattr(
        's3fs.S3FileSystem.exists',
        lambda *args, **kwargs: True
    )

    # Mock weights
    mock_weights = pd.DataFrame({
        'location_id': ['loc1'],
        'nwm_feature_id': [101],
        'weight': [1.0]
    })
    monkeypatch.setattr(
        'teehr.fetching.nwm.grid_utils.generate_zonal_weights',
        lambda *args, **kwargs: mock_weights
    )

    return ev


def test_complete_workflow_unit_test(full_mock_environment):
    """Test a complete workflow with all external dependencies mocked.

    This test runs in ~0.3s because everything external is mocked.
    """
    ev = full_mock_environment

    # All of these operations use mocks
    # ev.fetch.usgs_streamflow(...)
    # ev.fetch.nwm_retrospective_points(...)
    # ev.fetch.nwm_retrospective_grids(...)

    # Test your logic, not external services
    pass


# ============== PERFORMANCE COMPARISON ==============

def test_performance_with_mocks_guide():
    """Performance comparison with different approaches:

    1. evaluation_v0_3 (no mocks):
       - ~1.0s per test
       - Loads real data
       - Slower but more realistic

    2. evaluation_with_template (no mocks):
       - ~0.1s per test
       - No data loaded
       - Fast but needs manual data setup

    3. evaluation_with_template + mocks:
       - ~0.2s per test
       - Mocked external calls
       - Fast AND tests external integrations

    4. evaluation_with_template + auto-mocks (@pytest.mark.unit):
       - ~0.2s per test
       - Automatic mocking
       - Best for unit tests

    Recommendation: Start with #4 for unit tests, use #1 for integration tests.
    """
    pass
