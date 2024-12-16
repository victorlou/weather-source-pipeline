from unittest import TestCase, mock
from datetime import datetime, timedelta
import pytz

import pytest
import requests

from src.service.weather_api import (
    BaseWeatherAPI,
    HistoricalWeatherAPI,
    ForecastWeatherAPI,
    WeatherSourceAPI
)

class TestBaseWeatherAPI(TestCase):
    """Test the abstract base API class."""
    
    def setUp(self):
        class ConcreteAPI(BaseWeatherAPI):
            def get_data(self, latitude, longitude, start_date, end_date, fields=None):
                return self._make_request(
                    f"{self.base_url}/points/{latitude:.4f},{longitude:.4f}",
                    fields
                )
        
        with mock.patch("os.getenv") as mock_getenv:
            mock_getenv.return_value = "fake-api-key"
            self.api = ConcreteAPI(
                base_url="https://test.api.com",
                fields={"temp", "humidity"}
            )
    
    def test_validate_fields_success(self):
        result = self.api._validate_fields("temp,humidity")
        self.assertEqual(result, "temp,humidity")
    
    def test_validate_fields_invalid(self):
        with self.assertRaises(ValueError) as context:
            self.api._validate_fields("temp,invalid_field")
        self.assertIn("Invalid field names", str(context.exception))
    
    def test_validate_fields_empty(self):
        result = self.api._validate_fields(None)
        self.assertEqual(result, "popular")
    
    def test_get_timestamps(self):
        start_date = "2023-12-01"
        end_date = "2023-12-07"
        start_ts, end_ts = self.api._get_timestamps(start_date, end_date)
        
        # Verify timestamps are in correct format
        expected_start = datetime.strptime(
            f"{start_date} 00:00:00", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=pytz.UTC).isoformat()
        expected_end = datetime.strptime(
            f"{end_date} 23:00:00", "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=pytz.UTC).isoformat()
        
        self.assertEqual(start_ts, expected_start)
        self.assertEqual(end_ts, expected_end)
    
    def test_get_timestamps_invalid_date(self):
        with self.assertRaises(ValueError):
            self.api._get_timestamps("invalid-date", "2023-12-07")

class TestHistoricalWeatherAPI(TestCase):
    """Test the historical weather API client."""
    
    def setUp(self):
        with mock.patch("os.getenv") as mock_getenv:
            mock_getenv.return_value = "fake-api-key"
            self.api = HistoricalWeatherAPI()
    
    @mock.patch("requests.get")
    def test_get_data_success(self, mock_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [{"temperature": 20.5}]}
        mock_get.return_value = mock_response
        
        start_date = "2023-12-01"
        end_date = "2023-12-07"
        start_ts, end_ts = self.api._get_timestamps(start_date, end_date)
        
        result = self.api.get_data(
            latitude=40.7128,
            longitude=-74.0060,
            start_date=start_date,
            end_date=end_date,
            fields="temp,precip"
        )
        
        self.assertEqual(result, {"data": [{"temperature": 20.5}]})
        mock_get.assert_called_once()
        
        # Verify URL structure
        call_args = mock_get.call_args[0][0]
        expected_url = f"https://history.weathersourceapis.com/v2/points/40.7128,-74.0060/hours/{start_ts},{end_ts}"
        self.assertTrue(call_args.startswith("https://history.weathersourceapis.com/v2/points/"))
        self.assertIn(f"/hours/{start_ts},{end_ts}", call_args)
    
    def test_validate_historical_fields(self):
        valid_fields = "temp,precip,relHum"
        result = self.api._validate_fields(valid_fields)
        self.assertEqual(result, valid_fields)
        
        with self.assertRaises(ValueError):
            self.api._validate_fields("temp,invalid_field")

class TestForecastWeatherAPI(TestCase):
    """Test the forecast weather API client."""
    
    def setUp(self):
        with mock.patch("os.getenv") as mock_getenv:
            mock_getenv.return_value = "fake-api-key"
            self.api = ForecastWeatherAPI()
    
    @mock.patch("requests.get")
    def test_get_data_success(self, mock_get):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": [{"temperature": 18.3}]}
        mock_get.return_value = mock_response
        
        # Use future dates for forecast
        start_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=3)).strftime("%Y-%m-%d")
        start_ts, end_ts = self.api._get_timestamps(start_date, end_date)
        
        result = self.api.get_data(
            latitude=40.7128,
            longitude=-74.0060,
            start_date=start_date,
            end_date=end_date,
            fields="temp,precipProb"
        )
        
        self.assertEqual(result, {"data": [{"temperature": 18.3}]})
        mock_get.assert_called_once()
        
        # Verify URL structure
        call_args = mock_get.call_args[0][0]
        expected_url = f"https://forecast.weathersourceapis.com/v2/points/40.7128,-74.0060/hours/{start_ts},{end_ts}"
        self.assertTrue(call_args.startswith("https://forecast.weathersourceapis.com/v2/points/"))
        self.assertIn(f"/hours/{start_ts},{end_ts}", call_args)
    
    def test_validate_forecast_fields(self):
        valid_fields = "temp,precipProb,snowfallProb"
        result = self.api._validate_fields(valid_fields)
        self.assertEqual(result, valid_fields)
        
        with self.assertRaises(ValueError):
            self.api._validate_fields("temp,invalid_field")

class TestWeatherSourceAPI(TestCase):
    """Test the main Weather Source API facade."""
    
    def setUp(self):
        with mock.patch("os.getenv") as mock_getenv:
            mock_getenv.return_value = "fake-api-key"
            self.api = WeatherSourceAPI()
    
    def test_get_historical_weather(self):
        with mock.patch.object(HistoricalWeatherAPI, "get_data") as mock_get_data:
            mock_get_data.return_value = {"data": [{"temperature": 20.5}]}
            
            result = self.api.get_historical_weather(
                latitude=40.7128,
                longitude=-74.0060,
                start_date="2023-12-01",
                end_date="2023-12-07",
                fields="temp,precip"
            )
            
            self.assertEqual(result, {"data": [{"temperature": 20.5}]})
            mock_get_data.assert_called_once()
            args, kwargs = mock_get_data.call_args
            self.assertEqual(kwargs["latitude"], 40.7128)
            self.assertEqual(kwargs["longitude"], -74.0060)
            self.assertEqual(kwargs["start_date"], "2023-12-01")
            self.assertEqual(kwargs["end_date"], "2023-12-07")
            self.assertEqual(kwargs["fields"], "temp,precip")
    
    def test_get_forecast(self):
        with mock.patch.object(ForecastWeatherAPI, "get_data") as mock_get_data:
            mock_get_data.return_value = {"data": [{"temperature": 18.3}]}
            
            result = self.api.get_forecast(
                latitude=40.7128,
                longitude=-74.0060,
                start_date="2023-12-15",
                end_date="2023-12-18",
                fields="temp,precipProb"
            )
            
            self.assertEqual(result, {"data": [{"temperature": 18.3}]})
            mock_get_data.assert_called_once()
            args, kwargs = mock_get_data.call_args
            self.assertEqual(kwargs["latitude"], 40.7128)
            self.assertEqual(kwargs["longitude"], -74.0060)
            self.assertEqual(kwargs["start_date"], "2023-12-15")
            self.assertEqual(kwargs["end_date"], "2023-12-18")
            self.assertEqual(kwargs["fields"], "temp,precipProb") 