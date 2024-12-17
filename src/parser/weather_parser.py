import logging
from datetime import datetime
from typing import Dict, List, Union

import pandas as pd

# Set up module logger
logger = logging.getLogger(__name__)


class WeatherDataParser:
    """Parser for Weather Source API data."""
    
    @staticmethod
    def parse_historical_data(raw_data: Dict) -> pd.DataFrame:
        """
        Parse historical weather data into a pandas DataFrame.
        
        Args:
            raw_data: Raw API response from Weather Source historical endpoint
        
        Returns:
            Parsed DataFrame with weather data
        """
        try:
            if not raw_data.get("history"):
                logger.warning("No history data found in weather response")
                return pd.DataFrame()
            
            # Create DataFrame from history data
            df = pd.DataFrame(raw_data["history"])
            
            # Add location information
            if "location" in raw_data:
                df["latitude"] = raw_data["location"]["latitude"]
                df["longitude"] = raw_data["location"]["longitude"]
                df["timezone"] = raw_data["location"]["timezone"]
                df["elevation"] = raw_data["location"]["elevation"]
                df["country_code"] = raw_data["location"]["countryCode"]
                df["country_name"] = raw_data["location"]["countryName"]
            
            # Convert timestamp to datetime
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Define columns to exclude from string conversion
            exclude_columns = ["timestamp"]
            
            # Convert all columns to strings except excluded ones
            for column in df.columns:
                if column not in exclude_columns:
                    df[column] = df[column].astype(str)
            
            # Add metadata columns
            df["data_type"] = "historical"
            df["processed_at"] = datetime.now()
            
            return df
        
        except Exception as e:
            logger.error(f"Error parsing historical weather data: {str(e)}")
            logger.debug(f"Raw data structure: {raw_data.keys()}")
            raise
    
    @staticmethod
    def parse_forecast_data(raw_data: Dict) -> pd.DataFrame:
        """
        Parse forecast weather data into a pandas DataFrame.
        
        Args:
            raw_data: Raw API response from Weather Source forecast endpoint
        
        Returns:
            Parsed DataFrame with forecast data
        """
        try:
            if not raw_data.get("forecast"):  # Assuming similar structure to historical
                logger.warning("No forecast data found in weather response")
                return pd.DataFrame()
            
            # Create DataFrame from forecast data
            df = pd.DataFrame(raw_data["forecast"])
            
            # Add location information if available
            if "location" in raw_data:
                df["latitude"] = raw_data["location"]["latitude"]
                df["longitude"] = raw_data["location"]["longitude"]
                df["timezone"] = raw_data["location"]["timezone"]
                df["elevation"] = raw_data["location"]["elevation"]
                df["country_code"] = raw_data["location"]["countryCode"]
                df["country_name"] = raw_data["location"]["countryName"]
            
            # Convert timestamp to datetime
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Add metadata columns
            df["data_type"] = "forecast"
            df["processed_at"] = datetime.now()
            
            return df
        
        except Exception as e:
            logger.error(f"Error parsing forecast weather data: {str(e)}")
            logger.debug(f"Raw data structure: {raw_data.keys()}")
            raise
    
    @staticmethod
    def validate_data(df: pd.DataFrame) -> bool:
        """
        Validate parsed weather data.
        
        Args:
            df: Parsed DataFrame to validate
        
        Returns:
            Boolean indicating if data is valid
        """
        try:
            # Check if DataFrame is empty
            if df.empty:
                logger.warning("Empty DataFrame detected")
                return False
            
            # Check for required columns
            required_columns = ["latitude", "longitude", "timestamp", "data_type", "processed_at"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                logger.warning(f"Missing required columns: {missing_columns}")
                return False
            
            # Check for null values in critical columns
            critical_columns = ["latitude", "longitude", "timestamp"]
            null_counts = df[critical_columns].isnull().sum()
            
            if null_counts.any():
                logger.warning(f"Null values found in critical columns: {null_counts}")
                return False
            
            return True
        
        except Exception as e:
            logger.error(f"Error validating weather data: {str(e)}")
            return False 