import requests
import pandas as pd
import os
import dotenv
import logging

# Load environment variables
dotenv.load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ---------------------------- #
#      API REQUEST HANDLER     #
# ---------------------------- #
def fetch_data_from_api(api_url, payload, api_key_env_var, method="POST", headers=None):
    """Generic function to fetch data from an API."""
    api_key = os.getenv(api_key_env_var)
    if not api_key:
        raise ValueError(f"{api_key_env_var} is not set! Check your .env file.")

    # Inject API key if needed
    if "registrationkey" in payload:
        payload["registrationkey"] = api_key

    headers = headers or {"Content-type": "application/json"}

    logging.info(f"Sending API request to {api_url}...")

    if method == "GET":
        response = requests.get(api_url, params=payload, headers=headers)
    else:  # Default to POST
        response = requests.post(api_url, json=payload, headers=headers)

    if response.status_code != 200:
        logging.error(f"API Request Failed: {response.status_code} - {response.text}")
        raise RuntimeError(f"API request failed with status code {response.status_code}")

    return response.json()

# ---------------------------- #
#     RESPONSE PROCESSING      #
# ---------------------------- #
def process_bls_api_response(json_data, required_fields):
    """Process BLS API response and return a DataFrame."""
    try:
        series = json_data.get("Results", {}).get("series", [])
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


def process_fred_api_response(json_data, required_fields):
    """Process FRED API response and return a DataFrame."""
    try:
        observations = json_data.get("observations", [])
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
#    BLS CPI DATA FETCHING     #
# ---------------------------- #
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
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

# ---------------------------- #
#   FRED PCE DATA FETCHING     #
# ---------------------------- #
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
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
        "observation_end": FRED_END_DATE
    }

    json_data = fetch_data_from_api(FRED_API_URL, payload, "FRED_API_KEY", method="GET")
    df = process_fred_api_response(json_data, ["date", "value"])
    save_data_to_csv(df, output_path)

# ---------------------------- #
#     MAIN FUNCTIONALITY       #
# ---------------------------- #
if __name__ == "__main__":
    fetch_cpi_data() # Default CPI data fetch
    fetch_pce_data()  
