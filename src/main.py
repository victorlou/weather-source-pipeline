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

from src.handler.weather_handler import WeatherDataHandler

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
        # Initialize handler (using local storage)
        handler = WeatherDataHandler(use_s3=False)
        
        # Process historical data
        saved_path = handler.process_historical_data(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            fields="temp,precip,relHum,snowfall",
            file_format="csv"  # or "parquet"
        )
        
        print(f"Data saved to: {saved_path}")
        
        # Display sample data
        import pandas as pd
        df = pd.read_csv(saved_path)
        print("\nSample of fetched data:")
        print(df.head())
        print("\nColumns in the data:")
        print(df.columns.tolist())
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return

if __name__ == "__main__":
    main() 