import os
import pytest
import requests
import requests_mock
import pandas as pd
from scripts.collect_data import fetch_cpi_data
import socket

socket.gethostbyname("api.bls.gov")

def test_fetch_cpi_data_success(monkeypatch, tmp_path):
    """Test successful CPI data retrieval and CSV saving."""

    mock_api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    mock_response = {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [{
                "data": [
                    {"year": "2024", "periodName": "January", "value": "260.474"},
                    {"year": "2024", "periodName": "February", "value": "261.582"},
                ]
            }]
        }
    }

    # Use monkeypatch to set a test API key
    monkeypatch.setenv("BLS_API_KEY", "test_api_key")

    # Use requests_mock to mock API response
    with requests_mock.Mocker() as mocker:
        mocker.post(mock_api_url, json=mock_response, status_code=200)

        # Run function with a temporary directory
        file_path = tmp_path / "cpi_data.csv"
        fetch_cpi_data(str(file_path))

        # Check if the CSV file was created
        assert file_path.exists()

        # Check CSV contents
        df = pd.read_csv(file_path)
        assert df.shape == (2, 3)  # 2 rows, 3 columns
        assert list(df.columns) == ["year", "periodName", "value"]
        assert df["value"].iloc[0] == 260.474

def test_fetch_cpi_data_api_failure(monkeypatch):
    """Test API failure scenario (e.g., API down, wrong endpoint)."""

    mock_api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

    # Use monkeypatch to set a test API key
    monkeypatch.setenv("BLS_API_KEY", "test_api_key")

    with requests_mock.Mocker() as mocker:
        mocker.post(mock_api_url, status_code=500, json={"message": "Internal Server Error"})

        with pytest.raises(RuntimeError, match="Error 500"):
            fetch_cpi_data()

def test_fetch_cpi_data_no_api_key(monkeypatch):
    """Test missing API key scenario."""

    # Use monkeypatch to remove the API key
    monkeypatch.delenv("BLS_API_KEY", raising=False)

    with pytest.raises(ValueError, match="BLS_API_KEY is not set! Check your .env file."):
        fetch_cpi_data()
















