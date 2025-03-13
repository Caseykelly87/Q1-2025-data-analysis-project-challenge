import pytest
import requests
from unittest.mock import MagicMock
import os

@pytest.fixture(scope="session")
def mock_bls_api_response():
    """Mock successful BLS API response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "Results": {
            "series": [
                {
                    "data": [
                        {"year": "2024", "periodName": "January", "value": "301.8"},
                        {"year": "2024", "periodName": "February", "value": "303.1"},
                    ]
                }
            ]
        }
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_ces_api_response():
    """Mock successful CES API response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 123,
        "message":"",
        "Results": {
            "series": [
                {
                    "seriesID": "CES0000000001",
                    "data": [
                        {
                            "year": "2024",
                            "period": "M12",
                            "periodName": "December",
                            "value": "158942",
                            "footnotes": [{}]
                        },
                    ]
                }
            ]
        }
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_fred_pce_api_response():
    """Mock successful FRED API response for PCE data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "realtime_start": "2025-03-05",
        "realtime_end": "2025-03-05",
        "observation_start": "2020-01-01",
        "observation_end": "2024-09-30",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 19,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-01-01", "value": "13885.947"},
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-04-01", "value": "12671.879"},
        ],
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_housing_api_response():
    """Mock successful FRED API response for housing data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "realtime_start": "2025-03-05",
        "realtime_end": "2025-03-05",
        "observation_start": "2020-01-01",
        "observation_end": "2024-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 50,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-01-01", "value": "322600"},
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-02-01", "value": "324500"},
        ],
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_ppi_api_response():
    """Mock successful FRED API response for PPI data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "realtime_start": "2025-03-05",
        "realtime_end": "2025-03-05",
        "observation_start": "2020-01-01",
        "observation_end": "2024-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 50,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-01-01", "value": "200.0"},
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-02-01", "value": "201.5"},
        ],
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_gdp_api_response():
    """Mock successful FRED API response for GDP data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "realtime_start": "2025-03-05",
        "realtime_end": "2025-03-05",
        "observation_start": "2020-01-01",
        "observation_end": "2024-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 20,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-01-01", "value": "21500.0"},
            {"realtime_start": "2025-03-05", "realtime_end": "2025-03-05", "date": "2020-04-01", "value": "19500.0"},
        ],
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_retail_sales_api_response():
    """Mock successful FRED API response for retail sales data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "realtime_start": "2025-03-11",
        "realtime_end": "2025-03-11",
        "observation_start": "2020-01-01",
        "observation_end": "2024-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 60,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {
                "realtime_start": "2025-03-11",
                "realtime_end": "2025-03-11",
                "date": "2020-01-01",
                "value": "528984",
            },
            {
                "realtime_start": "2025-03-11",
                "realtime_end": "2025-03-11",
                "date": "2020-02-01",
                "value": "533456",
            },
        ],
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_grocery_sales_api_response():
    """Mock successful FRED API response for grocery sales data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "realtime_start": "2025-03-11",
        "realtime_end": "2025-03-11",
        "observation_start": "2020-01-01",
        "observation_end": "2024-12-31",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 60,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {
                "realtime_start": "2025-03-11",
                "realtime_end": "2025-03-11",
                "date": "2020-01-01",
                "value": "67832",
            },
            {
                "realtime_start": "2025-03-11",
                "realtime_end": "2025-03-11",
                "date": "2020-02-01",
                "value": "68159",
            },
        ],
    }
    return mock_response

@pytest.fixture(scope="session")
def mock_median_household_income_api_response():
    """Mock successful FRED API response for median household income data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "realtime_start": "2025-03-11",
        "realtime_end": "2025-03-11",
        "observation_start": "1984-01-01",
        "observation_end": "2022-01-01",
        "units": "lin",
        "output_type": 1,
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 39,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {
                "realtime_start": "2025-03-11",
                "realtime_end": "2025-03-11",
                "date": "1984-01-01",
                "value": "46227",
            },
            {
                "realtime_start": "2025-03-11",
                "realtime_end": "2025-03-11",
                "date": "1985-01-01",
                "value": "47105",
            },
        ],
    }
    return mock_response

@pytest.fixture(scope="session")
def bls_api_config():
    """Fixture for BLS CPI API configuration."""
    return {
        "api_url": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "payload": {"seriesid": ["CUUR0000SA0"], "startyear": "2020", "endyear": "2024"},
        "api_key_env_var": "BLS_API_KEY",
    }

@pytest.fixture(scope="session")
def fred_api_config():
    """Fixture for FRED API configuration."""
    return {
        "api_url": "https://api.stlouisfed.org/fred/series/observations",
        "payload": {
            "series_id": "MRTSSM44X72USS",  # Set the series ID here
            "api_key": os.getenv("FRED_API_KEY"),
            "file_type": "json",
            "observation_start": "2020-01-01",
            "observation_end": "2024-12-31",
        },
        "api_key_env_var": "FRED_API_KEY",
    }

@pytest.fixture(scope="session")
def bls_required_fields():
    """Fixture for required fields in process_bls_api_response."""
    return ["year", "periodName", "value"]

@pytest.fixture(scope="session")
def fred_required_fields():
    """Fixture for required fields in process_fred_api_response."""
    return ["date", "value"]