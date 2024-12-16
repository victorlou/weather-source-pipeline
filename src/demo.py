#!/usr/bin/env python3
"""
Demo script to showcase the Weather Source ETL pipeline functionality.
Run this script to see the pipeline in action with example data.
"""

import os
import sys
import logging
from datetime import datetime, timedelta

from dotenv import load_dotenv

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.handler.weather_handler import WeatherDataHandler

# Load environment variables
load_dotenv()

# Set up module logger
logger = logging.getLogger(__name__)

def run_local_demo():
    """Run a demo using local storage."""
    logger.info("Running demo with local storage...")
    
    # Example coordinates for Washington, DC (a test location that works with evaluation API key)
    latitude = 38.8552
    longitude = -77.0513
    
    # Set up dates for historical data (just 24 hours for evaluation API)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    try:
        # Initialize handler with local storage
        handler = WeatherDataHandler(use_s3=False)
        
        # Process historical data
        logger.info("Processing historical weather data...")
        historical_path = handler.process_historical_data(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            fields="popular",  # Using popular field group (includes temp, precip, relHum, etc.)
            file_format="csv"  # Using CSV for easy viewing
        )
        logger.info(f"Historical data saved to: {historical_path}")
        
        # Process forecast data
        logger.info("Processing forecast weather data...")
        # Set up dates for forecast (next 3 days)
        forecast_start_date = datetime.now().strftime("%Y-%m-%d")
        forecast_end_date = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")
        
        forecast_path = handler.process_forecast_data(
            latitude=latitude,
            longitude=longitude,
            start_date=forecast_start_date,
            end_date=forecast_end_date,
            fields="temp,precip,precipProb,snowfall,snowfallProb",  # Using forecast-specific fields
            file_format="csv"
        )
        logger.info(f"Forecast data saved to: {forecast_path}")
        
        # Display sample data
        logger.info("\nSample data preview:")
        import pandas as pd
        historical_df = pd.read_csv(historical_path)
        forecast_df = pd.read_csv(forecast_path)
        
        logger.info("\nHistorical Data:")
        print(historical_df.head())
        logger.info("\nForecast Data:")
        print(forecast_df.head())
        
    except Exception as e:
        logger.error(f"Demo failed: {str(e)}")
        raise

def run_s3_demo():
    """Run a demo using S3 storage."""
    logger.info("Running demo with S3 storage...")
    
    # Check if AWS credentials are set
    required_env_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION", "S3_BUCKET_NAME"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.warning(f"Missing AWS credentials: {', '.join(missing_vars)}")
        logger.warning("Skipping S3 demo. Please set AWS credentials to run S3 demo.")
        return
    
    # Example coordinates for New York City (another test location)
    latitude = 40.7128
    longitude = -74.0060
    
    try:
        # Initialize handler with S3 storage
        handler = WeatherDataHandler(use_s3=True)
        
        # Process forecast data only for demo
        logger.info("Processing forecast weather data...")
        # Set up dates for forecast (next 3 days)
        forecast_start_date = datetime.now().strftime("%Y-%m-%d")
        forecast_end_date = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")
        
        forecast_path = handler.process_forecast_data(
            latitude=latitude,
            longitude=longitude,
            start_date=forecast_start_date,
            end_date=forecast_end_date,
            fields="popular",  # Using popular field group
            file_format="parquet"
        )
        logger.info(f"Forecast data saved to: {forecast_path}")
        
    except Exception as e:
        logger.error(f"S3 demo failed: {str(e)}")
        raise

def main():
    """Run the complete demo."""
    logger.info("=" * 50)
    logger.info("Starting Weather Source ETL Pipeline Demo")
    
    # Run local storage demo
    run_local_demo()
    
    logger.info("=" * 50)
    
    # Run S3 storage demo
    # run_s3_demo()
    
    logger.info("\nDemo completed successfully!")

if __name__ == "__main__":
    main() 