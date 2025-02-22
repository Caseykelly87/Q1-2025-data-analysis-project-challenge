import requests
import pandas as pd
import json
import dotenv
import os

# Load .env file
dotenv.load_dotenv()

# BLS API details
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
SERIES_ID = "CUUR0000SA0"  # CPI for All Urban Consumers
START_YEAR = "2020"
END_YEAR = "2024"


def fetch_cpi_data(output_path=None):
    
    # Set API KEY to BLS KEY stored in .env
    API_KEY = os.getenv("BLS_API_KEY")
    if not API_KEY:
        raise ValueError("BLS_API_KEY is not set! Check your .env file.")
    print(f"Using API Key: {API_KEY[:4]}********")
    
    """
    Fetch CPI data from the BLS API and save it to CSV.
    
    Parameters:
        output_path (str, optional): File path for saving the CPI data. 
                                     Defaults to `../data/raw/cpi_data.csv`.
    
    Raises:
        RuntimeError: If the API request fails.
    """

    headers = {"Content-type": "application/json"}
    data = {
        "seriesid": [SERIES_ID],
        "startyear": START_YEAR,
        "endyear": END_YEAR,
        "registrationkey": API_KEY  
    }

    print("||--||--SENDING API REQUEST--||--||")    
    response = requests.post(BLS_API_URL, json=data, headers=headers)

    # Default path (relative to script location)
    if output_path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_path = os.path.join(script_dir, "../data/raw/cpi_data.csv")

    output_path = os.path.abspath(output_path)  # Ensure absolute path

    if response.status_code == 200:
        json_data = response.json()
        
        # Extract CPI values
        series_data = json_data["Results"]["series"][0]["data"]
        df = pd.DataFrame(series_data)
        df = df[["year", "periodName", "value"]]
        
        # Ensure the directory exists before saving
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save to CSV
        df.to_csv(output_path, index=False)
        print(f"CPI data saved successfully to {output_path}")
    else:
        raise RuntimeError(f"Error {response.status_code}: {response.text}")
    
if __name__ == "__main__":
    fetch_cpi_data()  # Default path usage


