import pytest
import os
import pandas as pd
from aioresponses import aioresponses
import asyncio
from unittest.mock import patch, MagicMock
from scripts.collect_data import (
    fetch_data_async,
    process_bls_api_response,
    process_fred_api_response,
    process_fred_xml_response,
    save_data_to_csv,
    fetch_and_process_dataset,
)

# ---------------------------- #
#        SETUP FIXTURES        #
# ---------------------------- #

@pytest.fixture(scope="session", autouse=True)
def set_env_vars():
    """Mock API keys in environment variables."""
    os.environ["BLS_API_KEY"] = "mock_bls_key"
    os.environ["FRED_API_KEY"] = "mock_fred_key"
    os.environ["CENSUS_API_KEY"] = "mock_census_key"
    
@pytest.fixture
def mock_bls_json():
    """Mock BLS API JSON response."""
    return {
        "Results": {
            "series": [{
                "data": [
                    {"year": "2020", "periodName": "January", "value": "100"},
                    {"year": "2020", "periodName": "February", "value": "110"}
                ]
            }]
        }
    }

@pytest.fixture
def mock_fred_json():
    """Mock FRED API JSON response."""
    return {
        "observations": [
            {"date": "2020-01-01", "value": "100"},
            {"date": "2020-02-01", "value": "110"}
        ]
    }

@pytest.fixture
def mock_fred_xml():
    """Mock FRED API XML response."""
    xml_data = """
    <observations>
        <observation date="2020-01-01" value="100"/>
        <observation date="2020-02-01" value="110"/>
    </observations>
    """
    return xml_data

@pytest.fixture
def mock_fetch_data():
    async def mock_fetch(*args, **kwargs):
        if "bls.gov" in args[0]:
            return {
                "Results": {
                    "series": [{
                        "data": [
                            {"year": "2020", "periodName": "January", "value": "100"},
                            {"year": "2020", "periodName": "February", "value": "110"}
                        ]
                    }]
                }
            }
        elif "stlouisfed.org" in args[0]:
            return {
                "observations": [
                    {"date": "2020-01-01", "value": "100"},
                    {"date": "2020-02-01", "value": "110"}
                ]
            }
        return {}
    return mock_fetch

# ---------------------------- #
#        API FETCH TESTS       #
# ---------------------------- #

@pytest.mark.asyncio
async def test_fetch_data_async():
    """Test async API fetching using aioresponses."""
    mock_api_url = "https://api.mock.api/test"
    mock_response = {"key": "value"}

    with aioresponses() as m:
        m.post(mock_api_url, payload=mock_response)

        payload = {"param": "test"}
        response = await fetch_data_async(mock_api_url, payload, "BLS_API_KEY", method="POST")

        assert response == mock_response

@pytest.mark.asyncio
async def test_fetch_data_async_retry():
    """Test retry logic when API initially fails using aioresponses."""
    mock_api_url = "https://api.mock.api/test"

    with aioresponses() as m:
        m.post(mock_api_url, status=500)  # Fail first attempt
        m.post(mock_api_url, status=500)  # Fail second attempt
        m.post(mock_api_url, payload={"key": "value"})  # Succeed on third attempt

        payload = {"param": "test"}
        response = await fetch_data_async(mock_api_url, payload, "BLS_API_KEY", method="POST")

        assert response == {"key": "value"}

@pytest.mark.parametrize("api_url, payload, api_key_env_var, expected_response", [
    ("https://api.bls.gov/publicAPI/v2/timeseries/data/", {"seriesid": ["CUUR0000SA0"]}, "BLS_API_KEY", {"Results": {"series": [{"data": [{"year": "2020", "value": "100"}]}]}}),
    ("https://api.stlouisfed.org/fred/series/observations", {"series_id": "GDP"}, "FRED_API_KEY", {"observations": [{"date": "2020-01-01", "value": "100"}]}),
])
@pytest.mark.asyncio
async def test_fetch_data_async_param(api_url, payload, api_key_env_var, expected_response):
    """Test fetch_data_async with parameterized inputs."""
    with aioresponses() as m:
        m.post(api_url, payload=expected_response)
        response = await fetch_data_async(api_url, payload, api_key_env_var)
        assert response == expected_response


@pytest.mark.asyncio
async def test_concurrent_fetch_requests():
    """Test multiple concurrent API fetch requests."""
    mock_api_url = "https://api.mock.api/test"
    with aioresponses() as m:
        m.post(mock_api_url, payload={"key": "value"}, repeat=True)
        tasks = [
            fetch_data_async(mock_api_url, {"param": "test1"}, "BLS_API_KEY"),
            fetch_data_async(mock_api_url, {"param": "test2"}, "BLS_API_KEY"),
        ]
        results = await asyncio.gather(*tasks)
        assert all(result == {"key": "value"} for result in results), "All responses should match"

@pytest.mark.asyncio
async def test_fetch_data_async_invalid_payload():
    """Test how fetch_data_async handles invalid API payloads."""
    mock_api_url = "https://api.mock.api/test"
    with aioresponses() as m:
        m.post(mock_api_url, status=400)  # Simulate Bad Request
        payload = {"invalid_param": "bad_value"}
        with pytest.raises(RuntimeError, match="API request failed after"):
            await fetch_data_async(mock_api_url, payload, "BLS_API_KEY", method="POST")

@pytest.mark.asyncio
async def test_fetch_data_async_empty_response():
    """Test how fetch_data_async handles empty API responses."""
    mock_api_url = "https://api.mock.api/test"
    with aioresponses() as m:
        m.post(mock_api_url, payload={})  # Empty JSON
        payload = {"param": "test"}
        response = await fetch_data_async(mock_api_url, payload, "BLS_API_KEY", method="POST")
        assert response == {}, "Response should be empty JSON"


# ---------------------------- #
#    DATA PROCESSING TESTS     #
# ---------------------------- #

@pytest.mark.parametrize("function, response, required_fields, expected_columns, additional_args", [
    (process_bls_api_response, {"Results": {"series": [{"data": [{"year": "2020", "value": "100"}]}]}}, ["year", "value"], ["year", "value"], {"dataset_name": "CPI"}),
    (process_fred_api_response, {"observations": [{"date": "2020-01-01", "value": "100"}]}, ["date", "value"], ["date", "value"], None),
])
def test_process_api_response(function, response, required_fields, expected_columns, additional_args):
    """Parameterized test for process functions."""
    if additional_args:
        df = function(response, required_fields, **additional_args)
    else:
        df = function(response, required_fields)

    assert not df.empty
    assert list(df.columns) == expected_columns

def test_process_fred_xml_response(mock_fred_xml):
    """Test FRED XML response parsing."""
    from xml.etree.ElementTree import fromstring

    xml_root = fromstring(mock_fred_xml)
    df = process_fred_xml_response(xml_root, ["date", "value"])

    assert not df.empty, "DataFrame should not be empty"
    assert list(df.columns) == ["date", "value"], "Columns should be ['date', 'value']"
    assert df["value"].iloc[0] == 100, "First value should be 100"

def test_process_bls_api_response_missing_fields():
    """Test BLS response with missing required fields."""
    response = {"Results": {"series": [{"data": [{"periodName": "January"}]}]}}  # 'year' and 'value' are missing
    df = process_bls_api_response(response, ["year", "value", "periodName"], "CPI")
    assert df.empty, "DataFrame should be empty when required fields are missing"

def test_process_fred_api_response_malformed():
    """Test FRED response with malformed data."""
    response = {"observations": [{"date": "invalid_date", "value": "not_a_number"}]}
    df = process_fred_api_response(response, ["date", "value"])
    assert not df.empty, "DataFrame should still be created"
    assert pd.to_numeric(df["value"], errors="coerce").isna().iloc[0], "Value column should handle non-numeric gracefully"


# ---------------------------- #
#        FILE HANDLING         #
# ---------------------------- #

def test_save_data_to_csv(tmp_path):
    """Test saving DataFrame to a CSV file."""
    df = pd.DataFrame({"date": ["2020-01-01"], "value": [123]})
    output_file = tmp_path / "test.csv"

    save_data_to_csv(df, output_file)

    assert output_file.exists()

def test_save_data_to_csv_empty_dataframe(tmp_path):
    """Test saving an empty DataFrame."""
    df = pd.DataFrame(columns=["date", "value"])
    output_file = tmp_path / "empty_test.csv"
    save_data_to_csv(df, output_file)
    assert output_file.exists(), "Output file should still be created"
    loaded_df = pd.read_csv(output_file)
    assert loaded_df.empty, "Saved DataFrame should be empty"

def test_save_data_to_csv_non_writable_directory():
    """Test saving a DataFrame to a non-writable directory."""
    df = pd.DataFrame({"date": ["2020-01-01"], "value": [123]})
    with patch("os.makedirs", side_effect=PermissionError), \
         patch("pandas.DataFrame.to_csv", side_effect=PermissionError):
        with pytest.raises(PermissionError):
            save_data_to_csv(df, "/non_writable_dir/test.csv")


# ---------------------------- #
#       END-TO-END TESTS       #
# ---------------------------- #

@pytest.mark.asyncio
async def test_fetch_and_process_dataset(tmp_path, mock_fetch_data):
    """Test end-to-end fetch, process, and save dataset."""
    api_config = {
        "api_url": "https://api.bls.gov/publicAPI/v2/timeseries/data/",
        "api_key_env_var": "BLS_API_KEY",
        "datasets": {
            "CPI": {
                "payload": {
                    "seriesid": ["CUUR0000SA0"],
                    "startyear": "2020",
                    "endyear": "2024"
                },
                "required_fields": ["year", "value"]
            }
        }
    }
    output_path = tmp_path / "cpi_data.csv"

    with patch("scripts.collect_data.fetch_data_async", new=mock_fetch_data):
        await fetch_and_process_dataset(api_config, "CPI", output_path)

    assert output_path.exists()
    df = pd.read_csv(output_path)
    assert not df.empty
    assert list(df.columns) == ["year", "value"]
    assert df["value"].iloc[0] == 100

