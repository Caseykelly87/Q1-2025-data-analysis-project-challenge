import pandas as pd

# File paths
CPI_FILE = "data/raw/cpi_data.csv"
SALES_FILE = "data/raw/sales_data.csv"
MERGED_FILE = "data/processed/merged_data.csv"

def merge_data():
    """Load CPI and Sales data, transform months, and merge datasets."""
    # Load CPI data
    cpi_df = pd.read_csv(CPI_FILE)

    # Rename 'value' to 'CPI_U'
    cpi_df = cpi_df.rename(columns={"value": "CPI_U"})

    # Load Sales data
    sales_df = pd.read_csv(SALES_FILE)

    # Convert numeric month to full month name but keep the column name 'month'
    sales_df["month"] = sales_df["month"].apply(lambda x: pd.to_datetime(f"2024-{x}-01").strftime('%B'))

    # Merge datasets on 'year' and 'month' (renamed from 'periodName' in CPI)
    merged_df = sales_df.merge(cpi_df, left_on=["year", "month"], right_on=["year", "periodName"], how="left")

    # Drop the 'periodName' column from CPI since 'month' now serves the same purpose
    merged_df = merged_df.drop(columns=["periodName"])

    # Save the cleaned, merged data
    merged_df.to_csv(MERGED_FILE, index=False)
    print(f"Merged data saved to {MERGED_FILE}")

if __name__ == "__main__":
    merge_data()
