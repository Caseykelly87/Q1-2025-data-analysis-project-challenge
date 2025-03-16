import pytest
import os
from unittest.mock import AsyncMock

@pytest.fixture(scope="session", autouse=True)
def set_env_vars():
    """Mock API keys in environment variables."""
    os.environ["BLS_API_KEY"] = "mock_bls_key"
    os.environ["FRED_API_KEY"] = "mock_fred_key"

@pytest.fixture
def mock_fetch_data():
    """Fixture to mock the async fetch_data_async function."""
    async_mock = AsyncMock()
    async_mock.return_value = {"mock": "data"}
    return async_mock
