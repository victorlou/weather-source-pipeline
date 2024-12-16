import os
import logging
from abc import ABC, abstractmethod
from typing import Dict, Optional, Set
from datetime import datetime
import pytz
from urllib.parse import quote

import requests

from src.helper.utils import getenv_or_raise, validate_date_format

# Set up module logger
logger = logging.getLogger(__name__)

class BaseWeatherAPI(ABC):
    """Base class for Weather Source API endpoints."""
    
    def __init__(self, base_url: str, fields: Set[str]):
        """
        Initialize base API client.
        
        Args:
            base_url: Base URL for the API endpoint
            fields: Set of valid fields for this endpoint
        """
        self.api_key = getenv_or_raise("WEATHER_SOURCE_API_KEY")
        self.base_url = base_url
        self.valid_fields = fields
        self.headers = {
            "X-API-KEY": self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    def _encode_params(self, params: Dict) -> str:
        """Custom parameter encoder that preserves commas in fields parameter."""
        encoded_parts = []
        for key, value in params.items():
            if key == "fields":
                encoded_parts.append(f"{key}={value}")
            else:
                encoded_parts.append(f"{key}={quote(str(value))}")
        return "&".join(encoded_parts)
    
    def _validate_fields(self, fields: Optional[str]) -> str:
        """Validate and normalize field names."""
        if not fields:
            return "popular"
            
        field_list = [f.strip() for f in fields.split(",")]
        invalid_fields = [f for f in field_list if f not in self.valid_fields]
        
        if invalid_fields:
            raise ValueError(
                f"Invalid field names for {self.__class__.__name__}: "
                f"{', '.join(invalid_fields)}. Available fields: {', '.join(sorted(self.valid_fields))}"
            )
            
        return ",".join(field_list)
    
    def _get_timestamps(self, start_date: str, end_date: str) -> tuple[str, str]:
        """Convert dates to UTC timestamps in RFC3339 format."""
        if not all([validate_date_format(start_date), validate_date_format(end_date)]):
            raise ValueError("Dates must be in YYYY-MM-DD format")
        
        start_dt = datetime.strptime(f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        end_dt = datetime.strptime(f"{end_date} 23:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        
        return start_dt.isoformat(), end_dt.isoformat()
    
    def _make_request(self, url: str, fields: Optional[str] = None) -> Dict:
        """Make HTTP request to the API."""
        try:
            # Add fields as query parameter if specified
            if fields:
                params = {"fields": self._validate_fields(fields)}
                url = f"{url}?{self._encode_params(params)}"
            
            logger.debug(f"Making request to: {url}")
            logger.debug(f"Fields requested: {fields if fields else 'all'}")
            
            response = requests.get(url, headers=self.headers)
            
            # Log response details
            logger.debug(f"Response status code: {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            
            try:
                response_json = response.json()
                logger.debug(f"Response content: {response_json}")
            except Exception as e:
                logger.error(f"Failed to parse response as JSON: {str(e)}")
                logger.debug(f"Raw response content: {response.text}")
            
            if response.status_code == 401:
                raise ValueError("Invalid API key or unauthorized access")
            elif response.status_code == 400:
                error_msg = response.json().get('message', 'Bad request')
                raise ValueError(f"Bad request: {error_msg}")
            elif response.status_code == 429:
                raise ValueError("Rate limit exceeded")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from {self.__class__.__name__}: {str(e)}")
            logger.debug(f"Response content: {getattr(e.response, 'text', 'No response content')}")
            raise
    
    def _validate_dates(self, start_date: str, end_date: str) -> None:
        """Validate dates based on endpoint type."""
        today = datetime.now(pytz.UTC).date()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        if isinstance(self, HistoricalWeatherAPI):
            if start_dt > today or end_dt > today:
                raise ValueError("Historical data can only be requested for past dates")
        elif isinstance(self, ForecastWeatherAPI):
            if start_dt < today or end_dt < today:
                raise ValueError("Forecast data can only be requested for future dates")
    
    @abstractmethod
    def get_data(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: Optional[str] = None
    ) -> Dict:
        """Abstract method to get weather data."""
        pass

class HistoricalWeatherAPI(BaseWeatherAPI):
    """Client for historical weather data."""
    
    # Available field values for historical endpoint
    HISTORICAL_FIELDS = {
        'provisionalFlag', 'cldCvr', 'dewPt', 'feelsLike', 'heatIndex', 'mslPres',
        'precip', 'presTend', 'radSolar', 'relHum', 'sfcPres', 'snowfall', 'spcHum',
        'temp', 'vis', 'wetBulb', 'windChill', 'windDir', 'windDir80m', 'windDir100m',
        'windSpd', 'windSpd80m', 'windSpd100m', 'freezingRainFlag', 'icePelletsFlag',
        'rainFlag', 'snowFlag', 'all', 'allCldCvr', 'allHum', 'allPrecip', 'allPres',
        'allRad', 'allTemp', 'allWind', 'popular'
    }
    
    def __init__(self):
        super().__init__(
            base_url="https://history.weathersourceapis.com/v2",
            fields=self.HISTORICAL_FIELDS
        )
    
    def get_data(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: Optional[str] = None
    ) -> Dict:
        """Get historical weather data."""
        self._validate_dates(start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps(start_date, end_date)
        url = f"{self.base_url}/points/{latitude},{longitude}/hours/{start_timestamp},{end_timestamp}"
        return self._make_request(url, fields)

class ForecastWeatherAPI(BaseWeatherAPI):
    """Client for forecast weather data."""
    
    # Available field values for forecast endpoint
    FORECAST_FIELDS = {
        'timestampInit', 'cldCvr', 'dewPt', 'feelsLike', 'heatIndex', 'mslPres',
        'precip', 'precipProb', 'radSolar', 'relHum', 'sfcPres', 'snowfall',
        'snowfallProb', 'spcHum', 'temp', 'wetBulb', 'windChill', 'windDir',
        'windDir80m', 'windDir100m', 'windSpd', 'windSpd80m', 'windSpd100m',
        'all', 'allCldCvr', 'allHum', 'allPrecip', 'allPres', 'allRad',
        'allTemp', 'allWind', 'popular'
    }
    
    def __init__(self):
        super().__init__(
            base_url="https://forecast.weathersourceapis.com/v2",
            fields=self.FORECAST_FIELDS
        )
    
    def get_data(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: Optional[str] = None
    ) -> Dict:
        """Get forecast weather data."""
        self._validate_dates(start_date, end_date)
        start_timestamp, end_timestamp = self._get_timestamps(start_date, end_date)
        url = f"{self.base_url}/points/{latitude},{longitude}/hours/{start_timestamp},{end_timestamp}"
        return self._make_request(url, fields)

class WeatherSourceAPI:
    """Unified Weather Source API client."""
    
    def __init__(self):
        """Initialize both historical and forecast clients."""
        self.historical = HistoricalWeatherAPI()
        self.forecast = ForecastWeatherAPI()
    
    def get_historical_weather(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: Optional[str] = None
    ) -> Dict:
        """Get historical weather data."""
        # TEMPORARY MOCK DATA - REMOVE LATER
        # return {
        #     "location": {
        #         "latitude": latitude,
        #         "longitude": longitude,
        #         "timezone": "America/New_York",
        #         "countryCode": "US",
        #         "countryName": "United States of America",
        #         "subdivCode": "US-VA",
        #         "subdivName": "Virginia",
        #         "boundingPoints": [
        #             {
        #                 "onpointId": 10725864,
        #                 "latitude": latitude,
        #                 "longitude": longitude,
        #                 "grid": "NORTH_AMERICA_GRID",
        #                 "distance": 2.6772,
        #                 "elevation": 173.5
        #             }
        #         ],
        #         "grid": "NORTH_AMERICA_GRID",
        #         "elevation": 173.5
        #     },
        #     "timestampRange": {
        #         "timestampStart": "2019-12-20T23:00:00-05:00",
        #         "timestampEnd": "2019-12-20T23:00:00-05:00"
        #     },
        #     "fieldList": {
        #         "fields": {
        #             "timestamp": "Timestamp as string: RFC 3339",
        #             "temp": "Fahrenheit",
        #             "precip": "Inches",
        #             "relHum": "Percent value in [0,100]",
        #             "snowfall": "Inches"
        #         }
        #     },
        #     "history": [
        #         {
        #             "timestamp": "2019-12-20T23:00:00-05:00",
        #             "temp": 46.75,
        #             "precip": 0,
        #             "relHum": 96.87,
        #             "snowfall": 0
        #         }
        #     ]
        # }
    
        return self.historical.get_data(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            fields=fields
        )
    
    def get_forecast(
        self,
        latitude: float,
        longitude: float,
        start_date: str,
        end_date: str,
        fields: Optional[str] = None
    ) -> Dict:
        """Get forecast weather data."""
        # TEMPORARY MOCK DATA - REMOVE LATER
        # return {
        #     "location": {
        #         "latitude": latitude,
        #         "longitude": longitude,
        #         "timezone": "America/New_York",
        #         "countryCode": "US",
        #         "countryName": "United States of America",
        #         "subdivCode": "US-VA",
        #         "subdivName": "Virginia",
        #         "boundingPoints": [
        #             {
        #                 "onpointId": 10725864,
        #                 "latitude": latitude,
        #                 "longitude": longitude,
        #                 "grid": "NORTH_AMERICA_GRID",
        #                 "distance": 2.6772,
        #                 "elevation": 173.5
        #             }
        #         ],
        #         "grid": "NORTH_AMERICA_GRID",
        #         "elevation": 173.5
        #     },
        #     "timestampRange": {
        #         "timestampStart": "2019-12-20T23:00:00-05:00",
        #         "timestampEnd": "2019-12-20T23:00:00-05:00"
        #     },
        #     "fieldList": {
        #         "fields": {
        #             "timestamp": "Timestamp as string: RFC 3339",
        #             "timestampInit": "Timestamp as string: RFC 3339",
        #             "temp": "Fahrenheit",
        #             "precip": "Inches",
        #             "precipProb": "Percent value in [0,100]",
        #             "snowfall": "Inches",
        #             "snowfallProb": "Percent value in [0,100]"
        #         }
        #     },
        #     "forecast": [
        #         {
        #             "timestamp": "2019-12-20T23:00:00-05:00",
        #             "timestampInit": "2019-12-20T23:00:00-05:00",
        #             "temp": 46.75,
        #             "precip": 0,
        #             "precipProb": 0,
        #             "snowfall": 0,
        #             "snowfallProb": 0
        #         }
        #     ]
        # }

        return self.forecast.get_data(
            latitude=latitude,
            longitude=longitude,
            start_date=start_date,
            end_date=end_date,
            fields=fields
        )