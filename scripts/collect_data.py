import os
import logging
import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ---------------------------- #
#       API REQUEST HANDLER    #
# ---------------------------- #
import time

def fetch_data_from_api(api_url, payload, api_key_env_var, method="POST", headers=None, max_retries=3, retry_delay=5):
    """Generic function to fetch data from an API with retry logic."""
    api_key = os.getenv(api_key_env_var)
    if not api_key:
        raise ValueError(f"{api_key_env_var} is not set! Check your .env file.")

    # Inject API key if needed
    if "registrationkey" in payload:
        payload["registrationkey"] = api_key

    headers = headers or {"Content-type": "application/json"}

    for attempt in range(max_retries):
        try:
            logging.info(f"Sending API request to {api_url} (attempt {attempt + 1})...")

            if method == "GET":
                response = requests.get(api_url, params=payload, headers=headers)
            else:  # Default to POST
                response = requests.post(api_url, json=payload, headers=headers)

            if response.status_code == 200:
                return response.json()
            else:
                logging.error(f"API Request Failed: {response.status_code} - {response.text}")
                if attempt < max_retries - 1:
                    logging.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(f"API request failed after {max_retries} attempts with status code {response.status_code}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Network error: {e}")
            if attempt < max_retries - 1:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise RuntimeError(f"API request failed after {max_retries} attempts due to network errors.")


# ---------------------------- #
#      RESPONSE PROCESSING     #
# ---------------------------- #
def process_bls_api_response(json_data, required_fields):
    """Process BLS API response and return a DataFrame."""
    try:
        series = json_data.get("Results", {}).get("series",)
        if not series or not series[0].get("data"):
            logging.warning("Empty response from BLS API.")
            return pd.DataFrame(columns=required_fields)

        df = pd.DataFrame(series[0]["data"])

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        if df["value"].isnull().any():
            raise ValueError("Invalid data type in BLS API response")

        return df[required_fields]

    except (KeyError, IndexError) as e:
        logging.error(f"Unexpected BLS API response format: {e}")
        raise ValueError("Unexpected BLS API response structure.")

def process_ces_api_response(json_data, required_fields):
    """Process CES API response and return a DataFrame."""
    try:
        # Check if 'series' list is empty
        if not json_data['Results']['series']:
            return pd.DataFrame(columns=required_fields)  # Return empty DataFrame

        series = json_data['Results']['series'][0]
        series_id = series['seriesID']
        series_data = series['data']
        for item in series_data:
            item['seriesID'] = series_id
        df = pd.DataFrame(series_data)
        return df[required_fields]

    except (KeyError, IndexError) as e:
        logging.error(f"Unexpected CES API response format: {e}")
        raise ValueError("Unexpected CES API response structure.")

def process_fred_api_response(json_data, required_fields):
    """Process FRED API response and return a DataFrame."""
    try:
        observations = json_data.get("observations",)
        if not observations:
            logging.warning("Empty response from FRED API.")
            return pd.DataFrame(columns=required_fields)

        df = pd.DataFrame(observations)

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        if df["value"].isnull().any():
            raise ValueError("Invalid data type in FRED API response")

        return df[required_fields]

    except (KeyError, IndexError) as e:
        logging.error(f"Unexpected FRED API response format: {e}")
        raise ValueError("Unexpected FRED API response structure.")


# ---------------------------- #
#          DATA SAVING         #
# ---------------------------- #
def save_data_to_csv(df, output_path):
    """Save any DataFrame to a CSV file with proper directory handling."""
    output_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)
    logging.info(f"Data saved successfully to {output_path}")


# ---------------------------- #
#      BLS DATA FETCHING       #
# ---------------------------- #
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# CPI Consumer Price Index
CPI_SERIES_ID = "CUUR0000SA0"
START_YEAR = "2020"
END_YEAR = "2024"


def fetch_cpi_data(output_path="data/raw/cpi_data.csv"):
    """Fetch, process, and save CPI data."""
    payload = {
        "seriesid": [CPI_SERIES_ID],
        "startyear": START_YEAR,
        "endyear": END_YEAR
    }

    json_data = fetch_data_from_api(BLS_API_URL, payload, "BLS_API_KEY", method="POST")
    df = process_bls_api_response(json_data, ["year", "periodName", "value"])
    save_data_to_csv(df, output_path)

# CES Current Employment Statistics
CES_SERIES_ID = "CES0000000001"
CES_START_YEAR = "2020"
CES_END_YEAR = "2024"

def fetch_ces_data(output_path="data/raw/ces_data.csv"):
    """Fetch, process, and save CES data."""
    payload = {
        "seriesid": [CES_SERIES_ID],
        "startyear": CES_START_YEAR,
        "endyear": CES_END_YEAR,
    }

    ces_json_data = fetch_data_from_api(BLS_API_URL, payload, "BLS_API_KEY", method="POST")
    ces_df = process_ces_api_response(ces_json_data, ["seriesID", "year", "periodName", "value"])
    save_data_to_csv(ces_df, output_path)

# ---------------------------- #
#      FRED DATA FETCHING      #
# ---------------------------- #
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"

# PCE - PERSONAL CONSUMER EXPENDITURES
PCE_SERIES_ID = "PCECC96"
FRED_START_DATE = "2020-01-01"
FRED_END_DATE = "2024-09-30"


def fetch_pce_data(output_path="data/raw/pce_data.csv"):
    """Fetch, process, and save Real Personal Consumption Expenditures data."""
    payload = {
        "series_id": PCE_SERIES_ID,
        "api_key": os.getenv("FRED_API_KEY"),
        "file_type": "json",
        "observation_start": FRED_START_DATE,
        "observation_end": FRED_END_DATE,
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)


# HOUSING
HOUSING_SERIES_ID = "MSPUS"
HOUSING_START_DATE = "2020-01-01"
HOUSING_END_DATE = "2024-12-31"  


def fetch_housing_data(output_path="data/raw/housing_data.csv"):
    """Fetch, process, and save housing market data."""
    payload = {
        "series_id": HOUSING_SERIES_ID,
        "api_key": os.getenv("FRED_API_KEY"),
        "file_type": "json",
        "observation_start": HOUSING_START_DATE,
        "observation_end": HOUSING_END_DATE,
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)


# PPI - Producer Price Index
PPI_SERIES_ID = "PPIACO" 
PPI_START_DATE = "2020-01-01"
PPI_END_DATE = "2024-12-31"


def fetch_ppi_data(output_path="data/raw/ppi_data.csv"):
    """Fetch, process, and save PPI data."""
    payload = {
        "series_id": PPI_SERIES_ID,
        "api_key": os.getenv("FRED_API_KEY"),
        "file_type": "json",
        "observation_start": PPI_START_DATE,
        "observation_end": PPI_END_DATE,
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)


# GDP - Gross Domestic Product
GDP_SERIES_ID = "GDP"
GDP_START_DATE = "2020-01-01"
GDP_END_DATE = "2024-12-31"


def fetch_gdp_data(output_path="data/raw/gdp_data.csv"):
    """Fetch, process, and save GDP data."""
    payload = {
        "series_id": GDP_SERIES_ID,
        "api_key": os.getenv("FRED_API_KEY"),
        "file_type": "json",
        "observation_start": GDP_START_DATE,
        "observation_end": GDP_END_DATE,
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)

# RETAIL SALES
RETAIL_SALES_SERIES_ID = "MRTSSM44X72USS"
RETAIL_SALES_START_DATE = "2020-01-01"
RETAIL_SALES_END_DATE = "2024-12-31"


def fetch_retail_sales_data(output_path="data/raw/retail_sales.csv"):
    """Fetch, process, and save retail sales data."""
    payload = {
        "series_id": RETAIL_SALES_SERIES_ID,
        "api_key": os.getenv("FRED_API_KEY"),
        "file_type": "json",
        "observation_start": RETAIL_SALES_START_DATE,
        "observation_end": RETAIL_SALES_END_DATE,
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)


# GROCERY SALES
GROCERY_SALES_SERIES_ID = "MRTSSM4451USS"
GROCERY_SALES_START_DATE = "2020-01-01"
GROCERY_SALES_END_DATE = "2024-12-31"

def fetch_grocery_sales_data(output_path="data/raw/grocery_sales.csv"):
    """Fetch, process, and save grocery sales data."""
    payload = {
        "series_id": GROCERY_SALES_SERIES_ID,
        "api_key": os.getenv("FRED_API_KEY"),
        "file_type": "json",
        "observation_start": GROCERY_SALES_START_DATE,
        "observation_end": GROCERY_SALES_END_DATE,
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)


# MEDIAN HOUSEHOLD INCOME
MEDIAN_HOUSEHOLD_INCOME_SERIES_ID = "MEHOINUSA646N"
MEDIAN_HOUSEHOLD_INCOME_START_DATE = "2020-01-01"
MEDIAN_HOUSEHOLD_INCOME_END_DATE = "2024-12-31"

def fetch_median_household_income_data(output_path="data/raw/median_household_income.csv"):
    """Fetch, process, and save median household income data."""
    payload = {
        "series_id": MEDIAN_HOUSEHOLD_INCOME_SERIES_ID,
        "api_key": os.getenv("FRED_API_KEY"),
        "file_type": "json",
        "observation_start": MEDIAN_HOUSEHOLD_INCOME_START_DATE,
        "observation_end": MEDIAN_HOUSEHOLD_INCOME_END_DATE,
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)

# ---------------------------- #
#      MAIN FUNCTIONALITY      #
# ---------------------------- #
if __name__ == "__main__":
    fetch_cpi_data()
    fetch_pce_data()
    fetch_housing_data()
    fetch_ppi_data()
    fetch_gdp_data()
    fetch_ces_data()
    fetch_retail_sales_data()
    fetch_grocery_sales_data()
    fetch_median_household_income_data()
