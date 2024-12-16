import os
import logging
from typing import Optional

from src.parser.weather_parser import WeatherDataParser
from src.service.weather_api import WeatherSourceAPI
from src.loader.local import LocalLoader
from src.loader.s3 import S3Loader

# Set up module logger
logger = logging.getLogger(__name__)

class WeatherDataHandler:
    """Handler class to orchestrate the ETL pipeline."""
    
    def __init__(self, use_s3: bool = False):
        """
        Initialize the handler.
        
        Args:
            use_s3: Whether to use S3 storage (default: False)
        """
        self.api_client = WeatherSourceAPI()
        self.parser = WeatherDataParser()
        self.use_s3 = use_s3
        self.s3_loader = S3Loader() if use_s3 else None
        self.local_loader = LocalLoader() if not use_s3 else None
    
    def process_historical_data(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: Optional[str] = None,
        file_format: str = "parquet"
    ) -> str:
        """Process historical weather data."""
        try:
            # Fetch data
            raw_data = self.api_client.get_historical_weather(
                latitude=latitude,
                longitude=longitude,
                start_date=start_date,
                end_date=end_date,
                fields=fields
            )
            
            # Parse data
            df = self.parser.parse_historical_data(raw_data)
            
            # Save data
            filename_prefix = f"weather_{latitude}_{longitude}"
            if self.use_s3:
                return self.s3_loader.upload_dataframe(
                    df,
                    filename_prefix=filename_prefix,
                    file_format=file_format,
                    folder="historical"
                )
            else:
                return self.local_loader.save_dataframe(
                    df,
                    filename_prefix=filename_prefix,
                    file_format=file_format
                )
            
        except Exception as e:
            logger.error(f"Error processing historical data: {str(e)}")
            raise
    
    def process_forecast_data(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: Optional[str] = None,
        file_format: str = "parquet"
    ) -> str:
        """Process forecast weather data."""
        try:
            # Fetch data
            raw_data = self.api_client.get_forecast(
                latitude=latitude,
                longitude=longitude,
                start_date=start_date,
                end_date=end_date,
                fields=fields
            )
            
            # Parse data
            df = self.parser.parse_forecast_data(raw_data)
            
            # Save data
            filename_prefix = f"weather_{latitude}_{longitude}"
            if self.use_s3:
                return self.s3_loader.upload_dataframe(
                    df,
                    filename_prefix=filename_prefix,
                    file_format=file_format,
                    folder="forecast"
                )
            else:
                return self.local_loader.save_dataframe(
                    df,
                    filename_prefix=filename_prefix,
                    file_format=file_format
                )
            
        except Exception as e:
            logger.error(f"Error processing forecast data: {str(e)}")
            raise