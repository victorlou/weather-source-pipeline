#!/usr/bin/env python3
"""
Main entry point for the Weather Source ETL pipeline.
"""

import os
import sys
import logging

import click
from dotenv import load_dotenv

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.handler.weather_handler import WeatherDataHandler
from src.helper.utils import validate_date_format

# Load environment variables
load_dotenv()

# Set up module logger
logger = logging.getLogger(__name__)

@click.command()
@click.option('--latitude', type=float, required=True, help='Location latitude')
@click.option('--longitude', type=float, required=True, help='Location longitude')
@click.option('--data-type', type=click.Choice(['historical', 'forecast']), 
              required=True, help='Type of weather data to process')
@click.option('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
@click.option('--fields', type=str, help='Comma-separated list of fields to retrieve')
@click.option('--file-format', type=click.Choice(['parquet', 'csv']), 
              default='parquet', help='Output file format')
@click.option('--use-s3', is_flag=True, help='Use S3 for data storage instead of local storage')
def main(latitude, longitude, data_type, start_date, end_date,
         fields, file_format, use_s3):
    """
    Main entry point for the ETL pipeline.
    
    Note: When using the evaluation API key, be mindful of rate limits.
    Consider using shorter date ranges and waiting between requests.
    """
    try:
        # Initialize handler
        handler = WeatherDataHandler(use_s3=use_s3)

        # Validate date format
        if not all([validate_date_format(start_date), validate_date_format(end_date)]):
            raise ValueError("Invalid date format. Use YYYY-MM-DD")
        
        # Process data based on type
        if data_type == "historical":
            logger.info("Processing historical weather data...")
            result = handler.process_historical_data(
                latitude=latitude,
                longitude=longitude,
                start_date=start_date,
                end_date=end_date,
                fields=fields,
                file_format=file_format
            )
            logger.info(f"Historical data saved to: {result}")
        else:  # forecast
            logger.info("Processing forecast weather data...")
            result = handler.process_forecast_data(
                latitude=latitude,
                longitude=longitude,
                start_date=start_date,
                end_date=end_date,
                fields=fields,
                file_format=file_format
            )
            logger.info(f"Forecast data saved to: {result}")
        
        logger.info("ETL pipeline completed successfully\n")
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise click.ClickException(str(e))

if __name__ == "__main__":
    main()
