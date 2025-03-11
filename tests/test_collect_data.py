import pytest
import requests
import pandas as pd
import os
import logging
from unittest.mock import patch, MagicMock

from scripts.collect_data import (
    fetch_data_from_api,
    process_bls_api_response,
    process_fred_api_response,
    save_data_to_csv,
    fetch_cpi_data,
    fetch_pce_data,
    fetch_housing_data,
    fetch_ppi_data,
    fetch_gdp_data,
    fetch_ces_data,
    process_ces_api_response,
    fetch_retail_sales_data,
    fetch_grocery_sales_data,
    fetch_median_household_income_data,
)

# Sample API responses
SAMPLE_BLS_API_RESPONSE = {
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

# Sample CES API response
SAMPLE_CES_API_RESPONSE = {
    "status": "REQUEST_SUCCEEDED",
    "responseTime": 123,
    "message": [],
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

SAMPLE_FRED_PCE_API_RESPONSE = {
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

SAMPLE_FRED_HOUSING_API_RESPONSE = {
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

SAMPLE_FRED_PPI_API_RESPONSE = {
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

SAMPLE_FRED_GDP_API_RESPONSE = {
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

SAMPLE_FRED_RETAIL_SALES_API_RESPONSE = {
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

SAMPLE_FRED_GROCERY_SALES_API_RESPONSE = {
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

SAMPLE_FRED_MEDIAN_HOUSEHOLD_INCOME_API_RESPONSE = {
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

@pytest.fixture
def mock_bls_api_response():
    """Mock successful BLS API response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_BLS_API_RESPONSE
    return mock_response

@pytest.fixture
def mock_fred_pce_api_response():
    """Mock successful FRED API response for PCE data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_FRED_PCE_API_RESPONSE
    return mock_response

@pytest.fixture
def mock_housing_api_response():
    """Mock successful FRED API response for housing data."""
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response.json = lambda: SAMPLE_FRED_HOUSING_API_RESPONSE
    return mock_response

@pytest.fixture
def mock_ppi_api_response():
    """Mock successful FRED API response for PPI data."""
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response.json = lambda: SAMPLE_FRED_PPI_API_RESPONSE
    return mock_response

@pytest.fixture
def mock_gdp_api_response():
    """Mock successful FRED API response for GDP data."""
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response.json = lambda: SAMPLE_FRED_GDP_API_RESPONSE
    return mock_response

@pytest.fixture
def bls_api_config():
    """Fixture for BLS CPI API configuration."""
    return {
        "api_url": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "payload": {"seriesid": ["CUUR0000SA0"], "startyear": "2020", "endyear": "2024"},
        "api_key_env_var": "BLS_API_KEY",
    }

@pytest.fixture
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

@pytest.fixture
def bls_required_fields():
    """Fixture for required fields in process_bls_api_response."""
    return ["year", "periodName", "value"]

@pytest.fixture
def fred_required_fields():
    """Fixture for required fields in process_fred_api_response."""
    return ["date", "value"]

@pytest.fixture
def mock_retail_sales_api_response():
    """Mock successful FRED API response for retail sales data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_FRED_RETAIL_SALES_API_RESPONSE
    return mock_response

@pytest.fixture
def mock_grocery_sales_api_response():
    """Mock successful FRED API response for grocery sales data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_FRED_GROCERY_SALES_API_RESPONSE
    return mock_response

@pytest.fixture
def mock_median_household_income_api_response():
    """Mock successful FRED API response for median household income data."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_FRED_MEDIAN_HOUSEHOLD_INCOME_API_RESPONSE
    return mock_response

def test_fetch_data_from_api_success_bls(mock_bls_api_response, bls_api_config):
    """Test BLS API call returns expected JSON data."""
    with patch("requests.post", return_value=mock_bls_api_response):
        data = fetch_data_from_api(**bls_api_config)
        assert "Results" in data
        assert "series" in data["Results"]
        assert isinstance(data["Results"]["series"], list)

def test_fetch_data_from_api_success_fred_pce(mock_fred_pce_api_response, fred_api_config):
    """Test FRED API call returns expected JSON data for PCE."""
    with patch("requests.get", return_value=mock_fred_pce_api_response):
        data = fetch_data_from_api(
            api_url=fred_api_config["api_url"],
            payload=fred_api_config["payload"],
            api_key_env_var=fred_api_config["api_key_env_var"],
            method="GET",
        )
        assert "observations" in data
        assert isinstance(data["observations"], list)

"""def test_fetch_data_from_api_success_fred_housing(mock_housing_api_response, fred_api_config):
    # Test FRED API call returns expected JSON data for Housing.
    with patch("requests.get", return_value=mock_housing_api_response):
        data = fetch"""

def test_fetch_data_from_api_failure_bls(caplog, bls_api_config):
    """Test BLS API failure handling."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("requests.post", return_value=mock_response):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="API request failed after 3 attempts with status code 500"):
                fetch_data_from_api(**bls_api_config)

    assert "API Request Failed: 500 - Internal Server Error" in caplog.text

def test_fetch_data_from_api_failure_fred(caplog, fred_api_config):
    """Test FRED API failure handling."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("requests.get", return_value=mock_response):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="API request failed after 3 attempts with status code 500"):
                fetch_data_from_api(
                    api_url=fred_api_config["api_url"],
                    payload=fred_api_config["payload"],
                    api_key_env_var=fred_api_config["api_key_env_var"],
                    method="GET",
                )

    assert "API Request Failed: 500 - Internal Server Error" in caplog.text

def test_fetch_data_from_api_missing_api_key_bls(bls_api_config):
    """Test missing BLS API key raises ValueError."""
    with patch.dict(os.environ, {bls_api_config["api_key_env_var"]: ""}):
        with pytest.raises(ValueError, match=f"{bls_api_config['api_key_env_var']} is not set! Check your .env file."):
            fetch_data_from_api(**bls_api_config)

def test_fetch_data_from_api_missing_api_key_fred(fred_api_config):
    """Test missing FRED API key raises ValueError."""
    with patch.dict(os.environ, {fred_api_config["api_key_env_var"]: ""}):
        with pytest.raises(ValueError, match=f"{fred_api_config['api_key_env_var']} is not set! Check your .env file."):
            fetch_data_from_api(
                api_url=fred_api_config["api_url"],
                payload=fred_api_config["payload"],
                api_key_env_var=fred_api_config["api_key_env_var"],
                method="GET",
            )

def test_process_bls_api_response_valid(bls_required_fields):
    """Test processing BLS API response into DataFrame."""
    df = process_bls_api_response(SAMPLE_BLS_API_RESPONSE, bls_required_fields)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == bls_required_fields
    assert len(df) == 2
    assert df.iloc[0]["value"] == 301.8

def test_process_fred_api_response_valid(fred_required_fields):
    """Test processing FRED API response into DataFrame."""
    df = process_fred_api_response(SAMPLE_FRED_HOUSING_API_RESPONSE, fred_required_fields)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == fred_required_fields
    assert len(df) == 2
    assert df.iloc[0]["value"] == 322600

def test_process_bls_api_response_empty(bls_required_fields):
    """Test empty BLS API response handling."""
    empty_response = {"Results": {"series": [{"data": []}]}}
    df = process_bls_api_response(empty_response, bls_required_fields)
    assert df.empty

def test_process_fred_api_response_empty(fred_required_fields):
    """Test empty FRED API response handling."""
    empty_response = {"observations": []}
    df = process_fred_api_response(empty_response, fred_required_fields)
    assert df.empty

def test_process_bls_api_response_invalid_format(bls_required_fields):
    """Test handling of unexpected BLS API response format."""
    df = process_bls_api_response({}, bls_required_fields)
    assert df.empty
    assert list(df.columns) == bls_required_fields

def test_process_fred_api_response_invalid_format(fred_required_fields):
    """Test handling of unexpected FRED API response format."""
    df = process_fred_api_response({}, fred_required_fields)
    assert df.empty
    assert list(df.columns) == fred_required_fields

def test_save_data_to_csv(tmp_path):
    """Test saving DataFrame to CSV."""
    df = pd.DataFrame({"year": ["2024"], "periodName": ["January"], "value": ["301.8"]})
    output_file = tmp_path / "cpi_test.csv"
    save_data_to_csv(df, output_file)
    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert list(saved_df.columns) == ["year", "periodName", "value"]

def test_fetch_cpi_data_end_to_end(tmp_path):
    """Integration test for full data collection pipeline for CPI."""
    output_file = tmp_path / "cpi_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_BLS_API_RESPONSE):
        fetch_cpi_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty

def test_process_ces_api_response_success():
    """Test successful processing of CES API response."""
    required_fields = ["seriesID", "year", "periodName", "value"]
    df = process_ces_api_response(SAMPLE_CES_API_RESPONSE, required_fields)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert all(col in df.columns for col in required_fields)

def test_process_ces_api_response_empty():
    """Test handling of empty CES API response."""
    empty_response = {"status": "REQUEST_SUCCEEDED", "responseTime": 456, "message": "", "Results": {"series": []}}
    required_fields = ["seriesID", "year", "periodName", "value"]
    df = process_ces_api_response(empty_response, required_fields)

    assert isinstance(df, pd.DataFrame)
    assert df.empty

def test_process_ces_api_response_missing_key():
    """Test handling of missing key in CES API response."""
    missing_key_response = {"status": "REQUEST_SUCCEEDED", "responseTime": 789, "message":""}  # Missing 'Results' key
    required_fields = ["seriesID", "year", "periodName", "value"]

    try:
        process_ces_api_response(missing_key_response, required_fields)
    except ValueError as e:
        assert str(e) == "Unexpected CES API response structure."
    else:
        assert False, "ValueError should have been raised"

# Test cases for fetch_ces_data
def test_fetch_ces_data_end_to_end(tmp_path):
    """Integration test for full data collection pipeline for CES data."""
    output_file = tmp_path / "ces_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_CES_API_RESPONSE):
        fetch_ces_data(output_path=output_file)

    assert os.path.exists(output_file)
    df = pd.read_csv(output_file)
    assert not df.empty
    assert all(col in df.columns for col in ["seriesID", "year", "periodName", "value"])

def test_fetch_pce_data_end_to_end(tmp_path):
    """Integration test for full data collection pipeline for PCE."""
    output_file = tmp_path / "pce_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_FRED_PCE_API_RESPONSE):
        fetch_pce_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty

def test_fetch_data_from_api_timeout_bls(bls_api_config):
    """Test BLS network timeout handling."""
    with patch("requests.post", side_effect=requests.exceptions.Timeout):
        with pytest.raises(RuntimeError, match="API request failed after 3 attempts due to network errors."):
            fetch_data_from_api(**bls_api_config)

def test_fetch_data_from_api_timeout_fred(fred_api_config):
    """Test FRED network timeout handling."""
    with patch("requests.get", side_effect=requests.exceptions.Timeout):
        with pytest.raises(RuntimeError, match="API request failed after 3 attempts due to network errors."):
            fetch_data_from_api(
                api_url=fred_api_config["api_url"],
                payload=fred_api_config["payload"],
                api_key_env_var=fred_api_config["api_key_env_var"],
                method="GET",
            )

def test_fetch_housing_data_end_to_end(tmp_path, mock_housing_api_response):
    """Integration test for full data collection pipeline for housing data."""
    output_file = tmp_path / "housing_full_test.csv"

    fetch_housing_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]

def test_fetch_ppi_data_end_to_end(tmp_path, mock_ppi_api_response):
    """Integration test for full data collection pipeline for PPI data."""
    output_file = tmp_path / "ppi_full_test.csv"

    with patch(
        "scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_FRED_PPI_API_RESPONSE
    ):
        fetch_ppi_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]

def test_fetch_gdp_data_end_to_end(tmp_path, mock_gdp_api_response):
    """Integration test for full data collection pipeline for GDP data."""
    output_file = tmp_path / "gdp_full_test.csv"

    with patch(
        "scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_FRED_GDP_API_RESPONSE
    ):
        fetch_gdp_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]

def test_fetch_data_from_api_success_fred_housing(mock_housing_api_response, fred_api_config):
    """Test FRED API call returns expected JSON data for Housing."""
    with patch("requests.get", return_value=mock_housing_api_response):
        data = fetch_data_from_api(
            api_url=fred_api_config["api_url"],
            payload=fred_api_config["payload"],  # Access payload directly
            api_key_env_var=fred_api_config["api_key_env_var"],
            method="GET",
        )
        assert "observations" in data
        assert isinstance(data["observations"], list)

def test_fetch_data_from_api_success_fred_ppi(mock_ppi_api_response, fred_api_config):
    """Test FRED API call returns expected JSON data for PPI."""
    with patch("requests.get", return_value=mock_ppi_api_response):
        data = fetch_data_from_api(
            api_url=fred_api_config["api_url"],
            payload=fred_api_config["payload"],  # Access payload directly
            api_key_env_var=fred_api_config["api_key_env_var"],
            method="GET",
        )
        assert "observations" in data
        assert isinstance(data["observations"], list)

def test_fetch_data_from_api_success_fred_gdp(mock_gdp_api_response, fred_api_config):
    """Test FRED API call returns expected JSON data for GDP."""
    with patch("requests.get", return_value=mock_gdp_api_response):
        data = fetch_data_from_api(
            api_url=fred_api_config["api_url"],
            payload=fred_api_config["payload"],  # Access payload directly
            api_key_env_var=fred_api_config["api_key_env_var"],
            method="GET",
        )
        assert "observations" in data
        assert isinstance(data["observations"], list)


# Test cases for fetch_retail_sales_data
def test_fetch_retail_sales_data_end_to_end(tmp_path, mock_retail_sales_api_response):
    """Integration test for full data collection pipeline for retail sales."""
    output_file = tmp_path / "retail_sales_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_FRED_RETAIL_SALES_API_RESPONSE):
        fetch_retail_sales_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]


# Test cases for fetch_grocery_sales_data
def test_fetch_grocery_sales_data_end_to_end(tmp_path, mock_grocery_sales_api_response):
    """Integration test for full data collection pipeline for grocery sales."""
    output_file = tmp_path / "grocery_sales_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_FRED_GROCERY_SALES_API_RESPONSE):
        fetch_grocery_sales_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]


# Test cases for fetch_median_household_income_data
def test_fetch_median_household_income_data_end_to_end(tmp_path, mock_median_household_income_api_response):
    """Integration test for full data collection pipeline for median household income."""
    output_file = tmp_path / "median_household_income_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_FRED_MEDIAN_HOUSEHOLD_INCOME_API_RESPONSE):
        fetch_median_household_income_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]