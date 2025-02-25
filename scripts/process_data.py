import pandas as pd

# File paths
CPI_FILE = "data/raw/cpi_data.csv"
SALES_FILE = "data/raw/sales_data.csv"
MERGED_FILE = "data/processed/merged_data.csv"


def load_data(file_path):
    """Load a CSV file into a Pandas DataFrame."""
    return pd.read_csv(file_path)

def preprocess_cpi_data(df):
    """Rename columns in the CPI dataset for clarity."""
    return df.rename(columns={"value": "CPI_U"})

def transform_sales_months(df):
    """Convert numeric months in sales data to full month names."""
    df["month"] = df["month"].apply(lambda x: pd.to_datetime(f"2024-{x}-01").strftime('%B'))
    return df

def merge_datasets(sales_df, cpi_df):
    """Merge CPI and sales data on year and month."""
    merged_df = sales_df.merge(cpi_df, left_on=["year", "month"], right_on=["year", "periodName"], how="left")
    merged_df = merged_df.drop(columns=["periodName"])  # Remove redundant column
    return merged_df

def save_data(df, file_path):
    """Save DataFrame to CSV."""
    df.to_csv(file_path, index=False)
    print(f"Merged data saved to {file_path}")

def merge_data():
    """Main function to load, process, merge, and save CPI and Sales data."""
    cpi_df = preprocess_cpi_data(load_data(CPI_FILE))
    sales_df = transform_sales_months(load_data(SALES_FILE))
    merged_df = merge_datasets(sales_df, cpi_df)
    save_data(merged_df, MERGED_FILE)

if __name__ == "__main__":
    merge_data()

