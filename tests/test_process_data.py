import unittest
from unittest.mock import patch, MagicMock, mock_open
import pandas as pd
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
        """Test that CSV loading returns a DataFrame."""
        df = load_data("fake_path.csv")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (2, 3))  # 2 rows, 3 columns

    def test_preprocess_cpi_data(self):
        """Test renaming of CPI value column."""
        processed_df = preprocess_cpi_data(CPI_DATA)
        self.assertIn("CPI_U", processed_df.columns)  # Ensure column was renamed
        self.assertNotIn("value", processed_df.columns)  # Ensure old name is removed

    def test_transform_sales_months(self):
        """Test numeric month transformation to full names."""
        transformed_df = transform_sales_months(SALES_DATA.copy())
        self.assertEqual(transformed_df["month"].tolist(), ["January", "February"])

    def test_merge_datasets(self):
        """Test merging of CPI and Sales datasets."""
        sales_transformed = transform_sales_months(SALES_DATA.copy())
        processed_cpi = preprocess_cpi_data(CPI_DATA.copy())
        merged_df = merge_datasets(sales_transformed, processed_cpi)

        # Verify merge success
        self.assertEqual(merged_df.shape[0], 2)  # Ensure rows match Sales data
        self.assertIn("CPI_U", merged_df.columns)  # CPI column must exist
        self.assertIn("sales", merged_df.columns)  # Sales column must exist
        self.assertNotIn("periodName", merged_df.columns)  # Ensure periodName was dropped

    @patch("builtins.open", new_callable=mock_open)
    def test_save_data(self, mock_file):
        """Test saving DataFrame to CSV without actually writing to disk."""
        dummy_df = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        save_data(dummy_df, "fake_path.csv")
        mock_file.assert_called_once_with("fake_path.csv", "w", newline="", encoding="utf-8", errors="strict")
        mock_file().write.assert_called()  # Ensure write method is called

if __name__ == "__main__":
    unittest.main()