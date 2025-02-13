import requests
import pandas as pd
import json
from dotenv import load_dotenv
import os

# Load .env file
load_dotenv()

# Get API key
API_KEY = os.getenv("BLS_API_KEY")

if not API_KEY:
    raise ValueError("BLS_API_KEY is not set! Check your .env file.")

print(f"Using API Key: {API_KEY[:4]}********")

# BLS API details
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SERIES_ID = "CUUR0000SA0"  # CPI for All Urban Consumers
START_YEAR = "2020"
END_YEAR = "2024"

def fetch_cpi_data():
    """Fetch CPI data from BLS API and save it to CSV."""
    headers = {"Content-type": "application/json"}
    data = {
        "seriesid": [SERIES_ID],
        "startyear": START_YEAR,
        "endyear": END_YEAR,
        "registrationkey": API_KEY  
    }

    response = requests.post(BLS_API_URL, json=data, headers=headers)

    file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/raw/cpi_data.csv"))

    if response.status_code == 200:
        json_data = response.json()
        
        # Extract CPI values
        series_data = json_data["Results"]["series"][0]["data"]
        df = pd.DataFrame(series_data)
        df = df[["year", "periodName", "value"]]
        
        # Save to CSV
        df.to_csv(file_path, index=False)
        print("CPI data saved successfully.")
    else:
        print(f"Error {response.status_code}: {response.text}")

if __name__ == "__main__":
    fetch_cpi_data()