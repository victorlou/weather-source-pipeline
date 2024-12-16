#!/usr/bin/env python3
"""
Simple script to fetch weather data from Weather Source API and save it locally.
"""

import os
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.loader.local import LocalLoader
from src.service.weather_api import WeatherSourceAPI
from src.parser.weather_parser import WeatherDataParser

# Load environment variables
load_dotenv()

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
        # Initialize components
        api_client = WeatherSourceAPI()
        parser = WeatherDataParser()
        loader = LocalLoader()
        
        # Fetch historical data
        raw_data = api_client.get_historical_weather(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            fields="temp,precip,relHum,snowfall"
        )
        
        # Parse data
        df = parser.parse_historical_data(raw_data)
        
        # Generate output path
        output_file = f"weather_data_{start_date}_to_{end_date}"
        
        # Save data using local loader
        saved_path = loader.save_dataframe(
            df,
            filename_prefix=output_file,
            file_format='csv'  # or 'parquet'
        )
        
        print(f"Data saved to: {saved_path}")
        print("\nSample of fetched data:")
        print(df.head())
        print("\nColumns in the data:")
        print(df.columns.tolist())
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return

if __name__ == "__main__":
    main() 