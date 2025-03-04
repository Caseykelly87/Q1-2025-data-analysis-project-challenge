import pytest
import pandas as pd
import logging
import matplotlib
matplotlib.use('Agg') # set non-GUI backend
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as stats
from unittest.mock import patch, MagicMock
from scripts.analyze_data import (
    load_data, display_data_info, get_department_sales, get_cpi_by_year,
    display_sample_data, plot_monthly_sales_trends, perform_time_series_analysis,
    plot_correlation_matrix, calculate_pearson_correlation
)

# Mock DataFrame for testing
df_mock = pd.DataFrame({
    "year": [2020, 2020, 2021, 2021],
    "month": ["January", "February", "January", "February"],
    "department": ["A", "B", "A", "B"],
    "total_sales": [1000, 1500, 1100, 1600],
    "CPI_U": [250, 255, 260, 265]
})

# Test: Load Data Success
@patch("pandas.read_csv", return_value=df_mock)
def test_load_data_success(mock_read_csv):
    df = load_data("dummy_path.csv")
    assert df is not None
    assert not df.empty
    assert "total_sales" in df.columns
    mock_read_csv.assert_called_once()

# Test: Load Data - File Not Found
@patch("pandas.read_csv", side_effect=FileNotFoundError)
def test_load_data_file_not_found(mock_read_csv, caplog):
    with caplog.at_level(logging.ERROR):
        df = load_data("non_existent.csv")
        assert df is None
        assert "not found" in caplog.text

# Test: Load Data - Empty File
@patch("pandas.read_csv", side_effect=pd.errors.EmptyDataError)
def test_load_data_empty_file(mock_read_csv, caplog):
    with caplog.at_level(logging.ERROR):
        df = load_data("empty.csv")
        assert df is None
        assert "No data found" in caplog.text

# Test: Get Department Sales
def test_get_department_sales(caplog):
    with caplog.at_level(logging.INFO):
        get_department_sales(df_mock)
        assert "Calculating total sales by department" in caplog.text

# Test: Get CPI By Year
def test_get_cpi_by_year(caplog):
    with caplog.at_level(logging.INFO):
        get_cpi_by_year(df_mock)
        assert "Calculating average CPI per year" in caplog.text

# Test: Display Sample Data
def test_display_sample_data(caplog):
    with caplog.at_level(logging.INFO):
        display_sample_data(df_mock)
        assert "Displaying sample data" in caplog.text

# Test: Plot Monthly Sales Trends
@patch("matplotlib.pyplot.show")
def test_plot_monthly_sales_trends(mock_show, caplog):
    with caplog.at_level(logging.INFO):
        plot_monthly_sales_trends(df_mock)
        assert "Plotting monthly sales trends" in caplog.text
        mock_show.assert_called_once()

# Test: Perform Time Series Analysis
@patch("matplotlib.pyplot.show")
@patch("statsmodels.tsa.seasonal.seasonal_decompose")
def test_perform_time_series_analysis(mock_decompose, mock_show, caplog):
    # Generate mock data for at least 24 months (2 full cycles)
    date_range = pd.date_range(start="2020-01-01", periods=24, freq='ME')
    sales_data = [1000 + (i % 12) * 50 for i in range(24)]  # Simulated seasonal trend

    # Create DataFrame with additional `year` and `month` columns
    df_mock = pd.DataFrame({
        "date": date_range,
        "year": date_range.year,
        "month": date_range.strftime("%B"),  # Full month name
        "total_sales": sales_data
    })
    
    df_mock.set_index("date", inplace=True)  # Keep date as index

    # Mock decomposition return value
    mock_decompose.return_value.plot.return_value = None

    with caplog.at_level(logging.INFO):
        perform_time_series_analysis(df_mock)
    
    assert "Performing time series analysis on sales data" in caplog.text


# Test: Plot Correlation Matrix
@patch("seaborn.heatmap")
@patch("matplotlib.pyplot.show")
def test_plot_correlation_matrix(mock_show, mock_heatmap, caplog):
    with caplog.at_level(logging.INFO):
        plot_correlation_matrix(df_mock)
        assert "Plotting correlation matrix" in caplog.text
        mock_heatmap.assert_called_once()
        mock_show.assert_called_once()

# Test: Calculate Pearson Correlation
@patch("scipy.stats.pearsonr", return_value=(0.8, 0.01))
def test_calculate_pearson_correlation(mock_pearsonr, caplog):
    with caplog.at_level(logging.INFO):
        calculate_pearson_correlation(df_mock)
        assert "Calculating Pearson correlation" in caplog.text
        mock_pearsonr.assert_called_once()