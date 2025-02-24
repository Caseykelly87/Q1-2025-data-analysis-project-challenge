import pytest
import requests
import pandas as pd
import os
from unittest.mock import patch, MagicMock
from scripts.collect_data import fetch_cpi_data_from_api, process_cpi_data, save_cpi_data_to_csv, fetch_cpi_data

# Sample API response JSON
SAMPLE_API_RESPONSE = {
    "Results": {
        "series": [{
            "data": [
                {"year": "2024", "periodName": "January", "value": "301.8"},
                {"year": "2024", "periodName": "February", "value": "303.1"}
            ]
        }]
    }
}


@pytest.fixture
def mock_api_response():
    """Mock successful API response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = SAMPLE_API_RESPONSE
    return mock_response


def test_fetch_cpi_data_from_api_success(mock_api_response):
    """Test API call returns expected JSON data."""
    with patch("requests.post", return_value=mock_api_response):
        data = fetch_cpi_data_from_api()
        assert "Results" in data
        assert "series" in data["Results"]
        assert isinstance(data["Results"]["series"], list)


def test_fetch_cpi_data_from_api_failure():
    """Test API failure handling."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    
    with patch("requests.post", return_value=mock_response):
        with pytest.raises(RuntimeError, match="API request failed with status code 500"):
            fetch_cpi_data_from_api()


def test_fetch_cpi_data_from_api_missing_api_key():
    """Test missing API key raises ValueError."""
    with patch.dict(os.environ, {"BLS_API_KEY": ""}):
        with pytest.raises(ValueError, match="BLS_API_KEY is not set!"):
            fetch_cpi_data_from_api()


def test_process_cpi_data_valid():
    """Test processing API response into DataFrame."""
    df = process_cpi_data(SAMPLE_API_RESPONSE)
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == ["year", "periodName", "value"]
    assert len(df) == 2  # Two data points exist
    assert df.iloc[0]["value"] == 301.8


def test_process_cpi_data_empty():
    """Test empty API response handling."""
    empty_response = {"Results": {"series": [{"data": []}]}}
    df = process_cpi_data(empty_response)
    assert df.empty


def test_process_cpi_data_invalid_format():
    """Test handling of unexpected API response format by checking if an empty DataFrame is returned."""
    empty_response = {}
    df = process_cpi_data(empty_response)

    # Ensure the DataFrame is empty and has the expected columns
    assert isinstance(df, pd.DataFrame)
    assert df.empty
    assert list(df.columns) == ["year", "periodName", "value"]

def test_save_cpi_data_to_csv(tmp_path):
    """Test saving DataFrame to CSV."""
    df = pd.DataFrame({"year": ["2024"], "periodName": ["January"], "value": ["301.8"]})
    output_file = tmp_path / "cpi_test.csv"
    save_cpi_data_to_csv(df, output_file)
    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert list(saved_df.columns) == ["year", "periodName", "value"]


def test_fetch_cpi_data_end_to_end(tmp_path, mock_api_response):
    """Integration test for full data collection pipeline."""
    output_file = tmp_path / "cpi_full_test.csv"
    
    with patch("scripts.collect_data.fetch_cpi_data_from_api", return_value=SAMPLE_API_RESPONSE):
        fetch_cpi_data(output_path=output_file)
    
    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty


def test_fetch_cpi_data_from_api_timeout():
    """Test network timeout handling."""
    with patch("requests.post", side_effect=requests.exceptions.Timeout):
        with pytest.raises(requests.exceptions.Timeout):
            fetch_cpi_data_from_api()


def test_process_cpi_data_invalid_types():
    """Test handling of invalid data types in API response."""
    invalid_response = {
        "Results": {
            "series": [{
                "data": [
                    {"year": "2024", "periodName": "January", "value": "not a number"}
                ]
            }]
        }
    }
    with pytest.raises(ValueError, match="Invalid data type in API response"):
        process_cpi_data(invalid_response)

















