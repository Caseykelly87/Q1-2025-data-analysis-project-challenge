import os
import logging
import pandas as pd
import asyncio
import aiohttp
from dotenv import load_dotenv
import json
import xml.etree.ElementTree as ET
import ssl
import certifi

# Load environment variables
load_dotenv()

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------- #
#       ASYNC API HANDLER      #
# ---------------------------- #
async def fetch_data_async(api_url, payload, api_key_env_var, method="POST", headers=None, max_retries=3, retry_delay=1):
    """Asynchronously fetch data from an API with retry logic."""
    # Set up SSL context using certifi to align with the behavior of `requests`
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(certifi.where())  # Use certifi's trusted CA bundle

    api_key = os.getenv(api_key_env_var)
    if not api_key:
        raise ValueError(f"{api_key_env_var} is not set! Check your .env file.")

    # Assign API key dynamically
    if "bls.gov" in api_url:
        payload["registrationkey"] = api_key  # BLS API expects "registrationkey"
    elif "stlouisfed.org" in api_url:
        payload["api_key"] = api_key  # FRED API expects "api_key"

    headers = headers or {"Content-type": "application/json"}

    for attempt in range(max_retries):
        try:
            logging.info(f"Sending API request to {api_url} (attempt {attempt + 1})...")
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
                if method == "GET":
                    async with session.get(api_url, params=payload, headers=headers) as response:
                        return await _handle_response(response)
                else:
                    async with session.post(api_url, json=payload, headers=headers) as response:
                        return await _handle_response(response)

        except aiohttp.ClientSSLError as ssl_error:
            logging.error(f"SSL error: {ssl_error}")
        except aiohttp.ClientError as e:
            logging.error(f"Network error: {e}")

        if attempt < max_retries - 1:
            logging.info(f"Retrying in {retry_delay} seconds...")
            await asyncio.sleep(retry_delay)

    raise RuntimeError(f"API request failed after {max_retries} attempts.")



async def _handle_response(response):
    """Handle the response from an HTTP request."""
    response_text = await response.text()
    if response.status == 200:
        try:
            return json.loads(response_text)  # JSON response
        except json.JSONDecodeError:
            return ET.fromstring(response_text)  # XML response
    else:
        logging.error(f"API Request Failed: {response.status} - {response_text}")
        response.raise_for_status()

# ---------------------------- #
#       RESPONSE PROCESSING    #
# ---------------------------- #
def process_bls_api_response(json_data, required_fields, dataset_name):
    """Process BLS API response and return a DataFrame."""
    try:
        series = json_data.get("Results", {}).get("series", [])
        if not series or not series[0].get("data"):
            logging.warning(f"Empty response from BLS API for dataset {dataset_name}.")
            return pd.DataFrame(columns=required_fields)

        df = pd.DataFrame(series[0]["data"])
        if dataset_name == "CES" and "seriesID" in series[0]:
            df["seriesID"] = series[0]["seriesID"]

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        return df[required_fields]

    except (KeyError, IndexError, ValueError) as e:
        logging.error(f"Unexpected BLS API response format for dataset {dataset_name}: {e}")
        return pd.DataFrame(columns=required_fields)

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

        return df[required_fields]

    except (KeyError, ValueError) as e:
        logging.error(f"Unexpected FRED API response format: {e}")
        return pd.DataFrame(columns=required_fields)
    
def process_fred_xml_response(xml_root, required_fields):
    """Process FRED API XML response and return a DataFrame."""
    try:
        observations = []
        for obs in xml_root.findall("observation"):
            obs_data = {attr: obs.attrib[attr] for attr in obs.attrib}
            observations.append(obs_data)

        df = pd.DataFrame(observations)

        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        return df[required_fields]

    except Exception as e:
        logging.error(f"Error processing FRED API XML response: {e}")
        return pd.DataFrame(columns=required_fields)

# ---------------------------- #
#         DATA SAVING          #
# ---------------------------- #
def save_data_to_csv(df, output_path):
    """Save any DataFrame to a CSV file with proper directory handling."""
    output_path = os.path.abspath(output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logging.info(f"Data saved successfully to {output_path}")

# ---------------------------- #
#    DATA FETCHING AND SAVE    #
# ---------------------------- #
async def fetch_and_process_dataset(api_config, dataset_name, output_path):
    """Fetch, process, and save a dataset based on API configuration."""
    dataset_config = api_config["datasets"].get(dataset_name)
    if not dataset_config:
        raise KeyError(f"Dataset '{dataset_name}' not found in configuration.")

    payload = dataset_config["payload"]
    required_fields = dataset_config.get("required_fields")
    if not required_fields:
        raise KeyError(f"'required_fields' missing for dataset: {dataset_name}")
    
    api_key_env_var = api_config["api_key_env_var"]
    method = api_config.get("method", "POST")

    response_data = await fetch_data_async(api_config["api_url"], payload, api_key_env_var, method=method)

    if isinstance(response_data, dict):
        if "bls.gov" in api_config["api_url"]:
            df = process_bls_api_response(response_data, required_fields, dataset_name)
        elif "fred" in api_config["api_url"]:
            df = process_fred_api_response(response_data, required_fields)
    elif isinstance(response_data, ET.Element):
        df = process_fred_xml_response(response_data, required_fields)
    else:
        raise ValueError("Unsupported response format from API.")

    save_data_to_csv(df, output_path)

# ---------------------------- #
#          MAIN SCRIPT         #
# ---------------------------- #
async def main():
    tasks = []
    for api_name, api_config in config.items():
        for dataset_name in api_config["datasets"]:
            output_path = f"data/raw/{dataset_name.lower()}_data.csv"
            tasks.append(fetch_and_process_dataset(api_config, dataset_name, output_path))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    with open("config.json", "r") as config_file:
        config = json.load(config_file)

    asyncio.run(main())
