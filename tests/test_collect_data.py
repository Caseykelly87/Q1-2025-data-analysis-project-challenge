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

def test_process_bls_api_response_valid(bls_required_fields, mock_bls_api_response):  # Pass the fixture as an argument
    """Test processing BLS API response into DataFrame."""
    df = process_bls_api_response(mock_bls_api_response.json(), bls_required_fields)  # Use mock_bls_api_response.json()
    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == bls_required_fields
    assert len(df) == 2
    assert df.iloc[0]["value"] == 301.8

def test_process_fred_api_response_valid(fred_required_fields, mock_housing_api_response):  # Pass the fixture as an argument
    """Test processing FRED API response into DataFrame."""
    df = process_fred_api_response(mock_housing_api_response.json(), fred_required_fields)  # Use mock_housing_api_response.json()
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

def test_fetch_cpi_data_end_to_end(tmp_path, mock_bls_api_response):
    """Integration test for full data collection pipeline for CPI."""
    output_file = tmp_path / "cpi_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_bls_api_response.json()):
        fetch_cpi_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty

def test_process_ces_api_response_success(mock_ces_api_response):
    """Test successful processing of CES API response."""
    required_fields = ["seriesID", "year", "periodName", "value"]
    df = process_ces_api_response(mock_ces_api_response.json(), required_fields)

    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert all(col in df.columns for col in required_fields)

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
def test_fetch_ces_data_end_to_end(tmp_path, mock_ces_api_response):
    """Integration test for full data collection pipeline for CES data."""
    output_file = tmp_path / "ces_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_ces_api_response.json()):
        fetch_ces_data(output_path=output_file)

    assert os.path.exists(output_file)
    df = pd.read_csv(output_file)
    assert not df.empty
    assert all(col in df.columns for col in ["seriesID", "year", "periodName", "value"])

def test_fetch_pce_data_end_to_end(tmp_path, mock_fred_pce_api_response):
    """Integration test for full data collection pipeline for PCE."""
    output_file = tmp_path / "pce_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_fred_pce_api_response.json()):
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

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_housing_api_response.json()):
        fetch_housing_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]

def test_fetch_ppi_data_end_to_end(tmp_path, mock_ppi_api_response):
    """Integration test for full data collection pipeline for PPI data."""
    output_file = tmp_path / "ppi_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_ppi_api_response.json()):
        fetch_pce_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]

def test_fetch_gdp_data_end_to_end(tmp_path, mock_gdp_api_response):
    """Integration test for full data collection pipeline for GDP data."""
    output_file = tmp_path / "gdp_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_gdp_api_response.json()):
        fetch_pce_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]

def test_fetch_data_from_api_success_fred_housing(mock_housing_api_response, fred_api_config):
    """Test FRED API call returns expected JSON data for Housing."""
    with patch("requests.get", return_value=mock_housing_api_response):
        data = fetch_data_from_api(
            api_url=fred_api_config["api_url"],
            payload=fred_api_config["payload"],
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
            payload=fred_api_config["payload"],
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
            payload=fred_api_config["payload"],
            api_key_env_var=fred_api_config["api_key_env_var"],
            method="GET",
        )
        assert "observations" in data
        assert isinstance(data["observations"], list)


# Test cases for fetch_retail_sales_data
def test_fetch_retail_sales_data_end_to_end(tmp_path, mock_retail_sales_api_response): 
    """Integration test for full data collection pipeline for retail sales."""
    output_file = tmp_path / "retail_sales_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_retail_sales_api_response.json()):
        fetch_retail_sales_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]


# Test cases for fetch_grocery_sales_data
def test_fetch_grocery_sales_data_end_to_end(tmp_path, mock_grocery_sales_api_response):
    """Integration test for full data collection pipeline for grocery sales."""
    output_file = tmp_path / "grocery_sales_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_grocery_sales_api_response.json()):
        fetch_grocery_sales_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]


# Test cases for fetch_median_household_income_data
def test_fetch_median_household_income_data_end_to_end(tmp_path, mock_median_household_income_api_response):
    """Integration test for full data collection pipeline for median household income."""
    output_file = tmp_path / "median_household_income_full_test.csv"

    with patch("scripts.collect_data.fetch_data_from_api", return_value=mock_median_household_income_api_response.json()):
        fetch_median_household_income_data(output_path=output_file)

    assert output_file.exists()
    saved_df = pd.read_csv(output_file)
    assert not saved_df.empty
    assert list(saved_df.columns) == ["date", "value"]