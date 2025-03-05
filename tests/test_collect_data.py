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

SAMPLE_FRED_API_RESPONSE = {
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

@pytest.fixture
def mock_bls_api_response():
    """Mock successful BLS API response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_BLS_API_RESPONSE
    return mock_response

@pytest.fixture
def mock_fred_api_response():
    """Mock successful FRED API response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_FRED_API_RESPONSE
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
            "series_id": "PCECC96",
            "api_key": os.getenv("FRED_API_KEY"),
            "file_type": "json",
            "observation_start": "2020-01-01",
            "observation_end": "2024-09-30",
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

# 2. Update Tests:

def test_fetch_data_from_api_success_bls(mock_bls_api_response, bls_api_config):
    """Test BLS API call returns expected JSON data."""
    with patch("requests.post", return_value=mock_bls_api_response):
        data = fetch_data_from_api(**bls_api_config)
        assert "Results" in data
        assert "series" in data["Results"]
        assert isinstance(data["Results"]["series"], list)

def test_fetch_data_from_api_success_fred(mock_fred_api_response, fred_api_config):
    """Test FRED API call returns expected JSON data."""
    with patch("requests.get", return_value=mock_fred_api_response):
        data = fetch_data_from_api(
            api_url=fred_api_config["api_url"],
            payload=fred_api_config["payload"],
            api_key_env_var=fred_api_config["api_key_env_var"],
            method="GET",
        )
        assert "observations" in data
        assert isinstance(data["observations"], list)

def test_fetch_data_from_api_failure_bls(caplog, bls_api_config):
    """Test BLS API failure handling."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("requests.post", return_value=mock_response):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="API request failed with status code 500"):
                fetch_data_from_api(**bls_api_config)

    assert "API Request Failed: 500 - Internal Server Error" in caplog.text

def test_fetch_data_from_api_failure_fred(caplog, fred_api_config):
    """Test FRED API failure handling."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("requests.get", return_value=mock_response):
        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError, match="API request failed with status code 500"):
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
    df = process_fred_api_response(SAMPLE_FRED_API_RESPONSE, fred_required_fields)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == fred_required_fields
    assert len(df) == 2
    assert df.iloc[0]["value"] == 13885.947

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

def test_fetch_pce_data_end_to_end(tmp_path):
    """Integration test for full data collection pipeline for PCE."""
    output_file = tmp_path / "pce_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=SAMPLE_FRED_API_RESPONSE):
        fetch_pce_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty

def test_fetch_data_from_api_timeout_bls(bls_api_config):
    """Test BLS network timeout handling."""
    with patch("requests.post", side_effect=requests.exceptions.Timeout):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_data_from_api(**bls_api_config)

def test_fetch_data_from_api_timeout_fred(fred_api_config):
    """Test FRED network timeout handling."""
    with patch("requests.get", side_effect=requests.exceptions.Timeout):
        with pytest.raises(requests.exceptions.Timeout): #Corrected line
            fetch_data_from_api(
                api_url=fred_api_config["api_url"],
                payload=fred_api_config["payload"],
                api_key_env_var=fred_api_config["api_key_env_var"],
                method="GET",
            )