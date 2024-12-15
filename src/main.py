#!/usr/bin/env python3
"""
Simple script to fetch weather data from Weather Source API and save it locally.
"""

import os
import sys
import json
from datetime import datetime, timedelta
import requests
import pandas as pd
from dotenv import load_dotenv

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.loader.local import LocalLoader

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

def process_data(data):
    """Convert API response to DataFrame."""
    if isinstance(data, list):
        df = pd.DataFrame.from_records(data)
    else:
        df = pd.DataFrame([data])
    return df

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
        # Initialize local loader
        loader = LocalLoader()
        
        # Fetch data
        weather_data = fetch_weather_data(latitude, longitude, start_date, end_date)
        
        # Process data into DataFrame
        df = process_data(weather_data)
        
        # Generate output path
        output_file = f"weather_data_{start_date}_to_{end_date}"
        
        # Save data using local loader
        saved_path = loader.save(
            df,
            filename=output_file,
            file_format='csv'  # or 'parquet'
        )
        
        print(f"Data saved to: {saved_path}")
        print("\nSample of fetched data:")
        print(df.head())
        print("\nColumns in the data:")
        print(df.columns.tolist())
        
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        print(f"Response content: {e.response.content if hasattr(e, 'response') else 'No response content'}")
        return
    except Exception as e:
        print(f"Error processing or saving data: {e}")
        return

if __name__ == "__main__":
    if not API_KEY:
        print("Error: WEATHER_SOURCE_API_KEY environment variable not set")
    else:
        main()