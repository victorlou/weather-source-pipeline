import os
import json
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# API Configuration
API_KEY = os.getenv('WEATHER_SOURCE_API_KEY')
BASE_URL = 'https://history.weathersourceapis.com/v2'
FIELDS = 'temp,precip,relHum,snowfall'

def get_iso_timestamp(date_str):
    """Convert date string to RFC3339 format timestamp with UTC offset."""
    # Parse the date string to datetime
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    # Format as RFC3339 with UTC offset
    return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

def fetch_weather_data(latitude, longitude, start_date, end_date):
    """Fetch weather data for a specific location and time period."""
    # Convert dates to RFC3339 timestamps
    start_timestamp = get_iso_timestamp(start_date)
    end_timestamp = get_iso_timestamp(end_date)
    
    url = f"{BASE_URL}/points/{latitude},{longitude}/hours/{start_timestamp},{end_timestamp}"
    
    headers = {
        'X-API-KEY': API_KEY
    }
    
    params = {
        'fields': FIELDS
    }
    
    print(f"Requesting URL: {url}")
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    data = response.json()
    print(f"Response data type: {type(data)}")
    print(f"Response data sample: {data[:1] if isinstance(data, list) else data}")
    
    return data

def save_data(data, output_file):
    """Save weather data to a local file."""
    # Convert list of dictionaries to DataFrame
    if isinstance(data, list):
        df = pd.DataFrame.from_records(data)
    else:
        # If it's a single dictionary or has a different structure
        df = pd.DataFrame([data])
    
    # Save as CSV for easy viewing
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")
    
    # Display sample of the data
    print("\nSample of fetched data:")
    print(df.head())
    print("\nColumns in the data:")
    print(df.columns.tolist())

def main():
    """Main function to demonstrate weather data fetching."""
    # Example coordinates (Washington, DC)
    latitude = 38.8552
    longitude = -77.0513
    
    # Get data for the last 7 days
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    print(f"Fetching weather data for coordinates ({latitude}, {longitude})")
    print(f"Time period: {start_date} to {end_date}")
    
    try:
        # Fetch data
        weather_data = fetch_weather_data(latitude, longitude, start_date, end_date)
        
        # Save to file
        output_file = f"weather_data_{start_date}_to_{end_date}.csv"
        save_data(weather_data, output_file)
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        print(f"Response content: {e.response.content if hasattr(e, 'response') else 'No response content'}")
        return

if __name__ == "__main__":
    if not API_KEY:
        print("Error: WEATHER_SOURCE_API_KEY environment variable not set")
    else:
        main() 