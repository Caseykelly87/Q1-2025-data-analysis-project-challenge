import requests
import pandas as pd
import os
import dotenv
import logging

# Load .env file
dotenv.load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# BLS API details
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SERIES_ID = "CUUR0000SA0"  # CPI for All Urban Consumers
START_YEAR = "2020"
END_YEAR = "2024"


def fetch_cpi_data_from_api():
    """Fetch CPI data from the BLS API."""
    API_KEY = os.getenv("BLS_API_KEY")
    if not API_KEY:
        raise ValueError("BLS_API_KEY is not set! Check your .env file.")

    headers = {"Content-type": "application/json"}
    data = {
        "seriesid": [SERIES_ID],
        "startyear": START_YEAR,
        "endyear": END_YEAR,
        "registrationkey": API_KEY
    }

    logging.info("Sending API request to BLS...")
    response = requests.post(BLS_API_URL, json=data, headers=headers)

    if response.status_code != 200:
        logging.error(f"API Request Failed: {response.status_code} - {response.text}")
        raise RuntimeError(f"API request failed with status code {response.status_code}")

    return response.json()


def process_cpi_data(json_data):
    """Extract and validate CPI data from API response."""
    try:
        series = json_data.get("Results", {}).get("series", [])
        if not series or not series[0].get("data"):
            logging.warning("Empty response from API.")
            return pd.DataFrame(columns=["year", "periodName", "value"])  # Return empty DataFrame

        df = pd.DataFrame(series[0]["data"])

        # Convert 'value' column to numeric type
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Validate data types
        if df["value"].isnull().any():
            raise ValueError("Invalid data type in API response")

        return df[["year", "periodName", "value"]]
    except (KeyError, IndexError) as e:
        logging.error(f"Unexpected API response format: {e}")
        raise ValueError("Unexpected API response structure.")



def save_cpi_data_to_csv(df, output_path):
    """Save CPI data to a CSV file."""
    output_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False)
    logging.info(f"CPI data saved successfully to {output_path}")


def fetch_cpi_data(output_path="data/raw/cpi_data.csv"):
    """Orchestrate the fetching, processing, and saving of CPI data."""
    json_data = fetch_cpi_data_from_api()
    df = process_cpi_data(json_data)
    save_cpi_data_to_csv(df, output_path)


if __name__ == "__main__":
    fetch_cpi_data()  # Default path usage



