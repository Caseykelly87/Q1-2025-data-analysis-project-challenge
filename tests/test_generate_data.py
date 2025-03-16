import random
import numpy as np
import pytest
import pandas as pd
from unittest import mock
from scripts.generate_data import generate_sales_data, save_sales_data,calculate_sales_units,get_department_data,load_cpi_data

@pytest.fixture(autouse=True)
def set_random_seed():
    #Automatically sets a fixed seed for all tests to ensure reproducibility.#
    np.random.seed(42)
    random.seed(42)
    
@pytest.fixture
def mock_cpi_dict():
    return {
        (2020, 1): 1.02,
        (2020, 2): 1.03,
        (2021, 1): 1.04,
    }


@pytest.fixture
def mock_file_write():
    #Mock open() so no actual file is created.#
    with mock.patch("builtins.open", mock.mock_open()) as mocked_open:
        yield mocked_open

def test_generate_sales_data_returns_dataframe(mock_cpi_dict):
    #Ensure function returns a DataFrame.#
    df = generate_sales_data(mock_cpi_dict)  # Pass cpi_dict
    assert isinstance(df, pd.DataFrame), "Expected a Pandas DataFrame"

def test_generate_sales_data_columns(mock_cpi_dict):
    #Ensure expected columns exist.#
    expected_columns = {"year", "month", "store_id", "department", "total_sales", "units_sold"}
    df = generate_sales_data(mock_cpi_dict)
    assert expected_columns.issubset(df.columns), "Missing expected columns"

def test_generate_sales_data_no_missing(mock_cpi_dict):
    #Ensure no missing values exist.#
    df = generate_sales_data(mock_cpi_dict)
    assert not df.isnull().values.any(), "Dataset contains missing values"

def test_generate_sales_data_values_valid(mock_cpi_dict):
    #Ensure sales and units_sold have valid values.#
    df = generate_sales_data(mock_cpi_dict)
    assert (df["total_sales"] >= 0).all(), "Total sales should not be negative"
    assert (df["units_sold"] >= 0).all(), "Units sold should not be negative"
    assert (df["units_sold"] <= 50000).all(), "Units sold is unrealistically high"

def test_generate_sales_data_empty_cpi_dict():
    #Test with an empty CPI dictionary.#
    empty_cpi_dict = {}
    df = generate_sales_data(empty_cpi_dict)
    assert not df.empty, "Expected non-empty DataFrame even with empty CPI dictionary"

def test_load_cpi_data_missing_values():
    #Test that load_cpi_data raises KeyError if any CPI value is missing.#
    mock_df = pd.DataFrame({
        'year': [2020, 2020, 2021],
        'periodName': ["January", "March", "January"], #removed february to create a missing month.
        'value': [1.02, 1.03, 1.04]
    })

    with mock.patch('pandas.read_csv', return_value=mock_df):
        with pytest.raises(KeyError, match=r"Missing CPI data for \(2020, 2\)"):
            load_cpi_data("dummy_path.csv")

def test_generate_sales_data_no_departments():
    #Test with no departments.#
    with mock.patch("scripts.generate_data.get_department_data") as mock_get_department_data:
        mock_get_department_data.return_value = ([], {}, {}, {}, {})
        df = generate_sales_data({})
        assert df.empty, "Expected empty DataFrame when no departments are defined"

def test_generate_sales_data_seasonality():
    #Ensure sales spike in highly seasonal months (e.g., December for Liquor).#
    df = generate_sales_data({}, start_year=2020, end_year=2020)
    liquor_december_sales = df[(df["department"] == "Liquor") & (df["month"] == 12)]["total_sales"].mean()
    liquor_july_sales = df[(df["department"] == "Liquor") & (df["month"] == 7)]["total_sales"].mean()
    assert liquor_december_sales > liquor_july_sales * 1.2, "Liquor sales should increase in December"

def test_generate_sales_data_single_month(mock_cpi_dict):
    #Test data generation for a single month.#
    df = generate_sales_data(mock_cpi_dict, start_year=2020, end_year=2020)
    assert df["year"].nunique() == 1, "Expected data for only one year"
    assert df["month"].nunique() == 12, "Expected data for 12 months"

def test_generate_sales_data_low_cpi():
    #Test with abnormally low CPI values (deflation scenario).#
    low_cpi_dict = {(2020, 1): 0.5, (2021, 1): 0.6, (2022, 1): 0.4}
    df = generate_sales_data(low_cpi_dict)
    assert (df["total_sales"] < 10000).any(), "Expected some total sales to be significantly low"

def test_generate_sales_data_high_cpi(mock_cpi_dict):
    #Test with abnormally high CPI values.#
    high_cpi_dict = {(2020, 1): 19.2, (2021, 1): 18.6, (2022, 1): 19.4}  # Abnormally high CPI multipliers
    df = generate_sales_data(high_cpi_dict)
    print(df["total_sales"].describe())
    assert (df["total_sales"] > 1000000).any(), "Expected some total sales to be exceptionally high"

def test_save_sales_data(mock_file_write):
    #Test the save_sales_data function.#
    df = pd.DataFrame({
        "year": [2020, 2020],
        "month": [1, 2],
        "store_id": [101, 102],
        "department": ["Produce", "Dairy"],
        "total_sales": [20000, 15000],
        "units_sold": [5000, 4000]
    })
    output_path = "data/test/sales_data.csv"
    save_sales_data(df, output_path)
    mock_file_write.assert_called_once_with(output_path, "w", encoding='utf-8', errors='strict', newline='')




