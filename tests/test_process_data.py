import unittest 
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
import warnings
from parameterized import parameterized
from scripts.process_data import (
    load_data, preprocess_cpi_data, transform_sales_months,
    merge_datasets, save_data
)

# Sample CPI Data
CPI_DATA = pd.DataFrame({
    "year": [2024, 2024],
    "periodName": ["January", "February"],
    "value": [300.5, 301.2]
})

# Sample Sales Data (numeric months)
SALES_DATA = pd.DataFrame({
    "year": [2024, 2024],
    "month": [1, 2],  # Will be converted to "January", "February"
    "sales": [10000, 12000]
})

class TestProcessData(unittest.TestCase):

    @patch("builtins.open", new_callable=mock_open, read_data="year,month,sales\n2024,1,10000\n2024,2,12000")
    def test_load_data(self, mock_file):
        df = load_data("fake_path.csv")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 3))  # 2 rows, 3 columns
    
    @patch("builtins.open", new_callable=mock_open, read_data="")
    def test_load_data_empty_file(self, mock_file):
        with self.assertRaises(ValueError) as cm:
            load_data("fake_path.csv")
        self.assertEqual(str(cm.exception), "fake_path.csv is empty or has no columns.")


    def test_preprocess_cpi_data(self):
        processed_df = preprocess_cpi_data(CPI_DATA.copy())
        self.assertIn("CPI_U", processed_df.columns)
        self.assertNotIn("value", processed_df.columns)
        self.assertIn("year", processed_df.columns)
        self.assertIn("month", processed_df.columns)
    

    def test_transform_sales_months(self):
        transformed_df = transform_sales_months(SALES_DATA.copy())
        self.assertEqual(transformed_df["month"].tolist(), ["January", "February"])
    
    def test_transform_sales_months_empty(self):
        df = pd.DataFrame(columns=["year", "month", "sales"])
        transformed_df = transform_sales_months(df)
        self.assertTrue(transformed_df.empty)  # Ensure function doesn't break

    def test_transform_sales_months_missing_column(self):
        df = pd.DataFrame({"year": [2024], "sales": [10000]})
        with self.assertRaises(KeyError) as cm:
            transform_sales_months(df)
        self.assertEqual(str(cm.exception), "'Missing required column: month'")

    @parameterized.expand([
        (CPI_DATA, SALES_DATA, 2),
        (pd.DataFrame(columns=["year", "month", "CPI_U"]), SALES_DATA, 2),  # Edge case: Empty CPI data
        (CPI_DATA, pd.DataFrame(columns=["year", "month", "sales"]), 0),  # Edge case: Empty sales data
        (pd.DataFrame(columns=["year", "month", "CPI_U"]), pd.DataFrame(columns=["year", "month", "sales"]), 0)  # Edge case: Both empty
    ])
    def test_merge_datasets(self, cpi_data, sales_data, expected_rows):
        if not sales_data.empty:
            sales_transformed = transform_sales_months(sales_data.copy())
        else:
            sales_transformed = sales_data
        
        if not cpi_data.empty:
            processed_cpi = preprocess_cpi_data(cpi_data.copy())
        else:
            processed_cpi = cpi_data

        merged_df = merge_datasets(sales_transformed, processed_cpi)
        self.assertEqual(merged_df.shape[0], expected_rows)

    @patch("builtins.open", new_callable=mock_open)
    def test_save_data(self, mock_file):
        dummy_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        save_data(dummy_df, "fake_path.csv")
        mock_file.assert_called_once_with("fake_path.csv", "w", newline="", encoding="utf-8", errors="strict")
        mock_file().write.assert_called()

if __name__ == "__main__":
    unittest.main()
