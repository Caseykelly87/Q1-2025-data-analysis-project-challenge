import pandas as pd
import logging
import os

# File paths
CPI_FILE = "data/raw/cpi_data.csv"
SALES_FILE = "data/raw/sales_data.csv"
MERGED_FILE = "data/processed/merged_data.csv"

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_data(file_path):
    # Loads data from a CSV file into a Pandas DataFrame.
    try:
        df = pd.read_csv(file_path)
        if df.empty:
            raise ValueError(f"{file_path} is empty.")
        return df
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}") from e
    except pd.errors.EmptyDataError as e:
        raise ValueError(f"{file_path} is empty or has no columns.") from e
    except pd.errors.ParserError as e:
        logging.error(f"Parsing error in file: {file_path}")
        raise ValueError(f"Parsing error in {file_path}") from e
    except UnicodeDecodeError as e:
        logging.error(f"Encoding error in {file_path}. Check the file encoding.")
        raise ValueError(f"Encoding error in {file_path}. Try using UTF-8.") from e



def preprocess_cpi_data(df):
    # Prepare CPI data for merging.
    return df.rename(columns={"value": "CPI_U", "periodName": "month"})


def transform_sales_months(df):
    # Convert numeric month to month name
    required_columns = {"year", "month"}
    
    missing = required_columns - set(df.columns)
    if missing:
        raise KeyError(f"Missing required column: {', '.join(missing)}")  # Remove single quotes around column names
    
    if df.empty:
        return df  

    df["month"] = df.apply(
        lambda row: pd.to_datetime(f"{row['year']}-{row['month']}-01").strftime('%B'),
        axis=1
    )
    return df
 
def merge_datasets(sales_df, cpi_df):
    required_columns = {"year", "month"}
    validate_columns(sales_df, required_columns)
    validate_columns(cpi_df, required_columns)

    merged_df = sales_df.merge(cpi_df, on=["year", "month"], how="left")
    return merged_df

def validate_columns(df, required_columns):
    # Ensures required columns exist in a DataFrame.
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise KeyError(f"Missing required columns: {missing_columns}")

def save_data(df, file_path):
    # Saves the DataFrame to a CSV file. 
    try:
        df.to_csv(file_path, index=False)
        logging.info(f"Merged data saved to {file_path}")
    except Exception as e:
        logging.error(f"Failed to save data to {file_path}: {e}")
        raise

def merge_data():
    # Main function to load, process, merge, and save datasets.
    logging.info("Loading CPI data...")
    cpi_df = preprocess_cpi_data(load_data(CPI_FILE))

    logging.info("Loading Sales data...")
    sales_df = transform_sales_months(load_data(SALES_FILE))

    logging.info("Merging datasets...")
    merged_df = merge_datasets(sales_df, cpi_df)

    logging.info("Saving merged data...")
    save_data(merged_df, MERGED_FILE)

if __name__ == "__main__":
    merge_data()
