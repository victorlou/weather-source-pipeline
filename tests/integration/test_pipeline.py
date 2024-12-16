import os
import tempfile
from datetime import datetime, timedelta
from unittest import TestCase, mock

import pandas as pd
import pytest
import requests
from botocore.exceptions import ClientError

from src.handler.weather_handler import WeatherDataHandler


class TestWeatherETLPipeline(TestCase):
    @classmethod
    def setUpClass(cls):
        # Set up environment variables for testing
        os.environ["WEATHER_SOURCE_API_KEY"] = "test-api-key"
        os.environ["AWS_ACCESS_KEY_ID"] = "test-aws-key"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "test-aws-secret"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["S3_BUCKET_NAME"] = "test-bucket"
        os.environ["LOG_LEVEL"] = "DEBUG"
        
        # Create temporary directory for local storage tests
        cls.temp_dir = tempfile.mkdtemp()
        os.environ["DATA_OUTPUT_PATH"] = cls.temp_dir

    def setUp(self):
        self.handler = WeatherDataHandler(use_s3=False)  # Use local storage for tests
        self.test_latitude = 40.7128
        self.test_longitude = -74.0060
        self.end_date = datetime.now().strftime("%Y-%m-%d")
        self.start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Clean any existing test files
        for file in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, file))

    @mock.patch("requests.get")
    @mock.patch("src.service.weather_api.WeatherSourceAPI")
    @mock.patch("src.parser.weather_parser.WeatherDataParser")
    def test_full_historical_pipeline(self, mock_parser_class, mock_api_class, mock_get):
        """Test the complete historical data pipeline."""
        # Mock requests response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "history": [
                {
                    "timestamp": "2023-12-01T00:00:00Z",
                    "temp": 20.5,
                    "precip": 0.0,
                    "relHum": 65
                }
            ],
            "location": {
                "latitude": self.test_latitude,
                "longitude": self.test_longitude,
                "timezone": "America/New_York",
                "elevation": 10,
                "countryCode": "US",
                "countryName": "United States"
            }
        }
        mock_get.return_value = mock_response
        
        # Set up API mock
        mock_api = mock.Mock()
        mock_api.get_historical_weather.return_value = mock_response.json()
        mock_api_class.return_value = mock_api
        
        # Set up parser mock
        mock_parser = mock.Mock()
        mock_df = pd.DataFrame({
            "timestamp": ["2023-12-01T00:00:00Z"],
            "temp": [20.5],
            "precip": [0.0],
            "relHum": [65],
            "latitude": [self.test_latitude],
            "longitude": [self.test_longitude],
            "data_type": ["historical"]
        })
        mock_parser.parse_historical_data.return_value = mock_df
        mock_parser_class.return_value = mock_parser
        
        # Update handler's components with our mocks
        self.handler.api_client = mock_api
        self.handler.parser = mock_parser
        
        try:
            file_path = self.handler.process_historical_data(
                latitude=self.test_latitude,
                longitude=self.test_longitude,
                start_date=self.start_date,
                end_date=self.end_date,
                fields="temp,precip,relHum",
                file_format="parquet"
            )
            
            # Verify the file exists
            self.assertTrue(os.path.exists(file_path))
            
            # Verify the data
            df = pd.read_parquet(file_path)
            self.assertFalse(df.empty)
            self.assertEqual(df["data_type"].iloc[0], "historical")
            self.assertEqual(df["temp"].iloc[0], 20.5)
            self.assertEqual(df["latitude"].iloc[0], self.test_latitude)
            self.assertEqual(df["longitude"].iloc[0], self.test_longitude)
            
            # Verify API call
            mock_api.get_historical_weather.assert_called_once()
            args, kwargs = mock_api.get_historical_weather.call_args
            self.assertEqual(kwargs["latitude"], self.test_latitude)
            self.assertEqual(kwargs["longitude"], self.test_longitude)
            self.assertEqual(kwargs["start_date"], self.start_date)
            self.assertEqual(kwargs["end_date"], self.end_date)
            self.assertEqual(kwargs["fields"], "temp,precip,relHum")
            
            # Verify parser call
            mock_parser.parse_historical_data.assert_called_once_with(
                mock_api.get_historical_weather.return_value
            )
        
        except Exception as e:
            self.fail(f"Pipeline failed with error: {str(e)}")

    @mock.patch("requests.get")
    @mock.patch("src.service.weather_api.WeatherSourceAPI")
    @mock.patch("src.parser.weather_parser.WeatherDataParser")
    def test_full_forecast_pipeline(self, mock_parser_class, mock_api_class, mock_get):
        """Test the complete forecast data pipeline."""
        # Mock requests response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "forecast": [
                {
                    "timestamp": "2023-12-15T00:00:00Z",
                    "temp": 18.3,
                    "precip": 0.2,
                    "precipProb": 30
                }
            ],
            "location": {
                "latitude": self.test_latitude,
                "longitude": self.test_longitude,
                "timezone": "America/New_York",
                "elevation": 10,
                "countryCode": "US",
                "countryName": "United States"
            }
        }
        mock_get.return_value = mock_response
        
        # Set up API mock
        mock_api = mock.Mock()
        mock_api.get_forecast.return_value = mock_response.json()
        mock_api_class.return_value = mock_api
        
        # Set up parser mock
        mock_parser = mock.Mock()
        mock_df = pd.DataFrame({
            "timestamp": ["2023-12-15T00:00:00Z"],
            "temp": [18.3],
            "precip": [0.2],
            "precipProb": [30],
            "latitude": [self.test_latitude],
            "longitude": [self.test_longitude],
            "data_type": ["forecast"]
        })
        mock_parser.parse_forecast_data.return_value = mock_df
        mock_parser_class.return_value = mock_parser
        
        # Update handler's components with our mocks
        self.handler.api_client = mock_api
        self.handler.parser = mock_parser
        
        try:
            file_path = self.handler.process_forecast_data(
                latitude=self.test_latitude,
                longitude=self.test_longitude,
                start_date=self.start_date,
                end_date=self.end_date,
                fields="temp,precip,precipProb",
                file_format="parquet"
            )
            
            # Verify the file exists
            self.assertTrue(os.path.exists(file_path))
            
            # Verify the data
            df = pd.read_parquet(file_path)
            self.assertFalse(df.empty)
            self.assertEqual(df["data_type"].iloc[0], "forecast")
            self.assertEqual(df["temp"].iloc[0], 18.3)
            self.assertEqual(df["precipProb"].iloc[0], 30)
            self.assertEqual(df["latitude"].iloc[0], self.test_latitude)
            self.assertEqual(df["longitude"].iloc[0], self.test_longitude)
            
            # Verify API call
            mock_api.get_forecast.assert_called_once()
            args, kwargs = mock_api.get_forecast.call_args
            self.assertEqual(kwargs["latitude"], self.test_latitude)
            self.assertEqual(kwargs["longitude"], self.test_longitude)
            self.assertEqual(kwargs["start_date"], self.start_date)
            self.assertEqual(kwargs["end_date"], self.end_date)
            self.assertEqual(kwargs["fields"], "temp,precip,precipProb")
            
            # Verify parser call
            mock_parser.parse_forecast_data.assert_called_once_with(
                mock_api.get_forecast.return_value
            )
        
        except Exception as e:
            self.fail(f"Pipeline failed with error: {str(e)}")

    @mock.patch("requests.get")
    @mock.patch("src.service.weather_api.WeatherSourceAPI")
    def test_pipeline_with_invalid_coordinates(self, mock_api, mock_get):
        """Test pipeline behavior with invalid coordinates."""
        # Mock requests to raise error
        mock_response = mock.Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"message": "Invalid coordinates"}
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "400 Client Error: Bad Request for url: test_url",
            response=mock_response
        )
        mock_get.return_value = mock_response
        
        with self.assertRaises(ValueError) as context:
            self.handler.process_historical_data(
                latitude=1000,  # Invalid latitude
                longitude=self.test_longitude,
                start_date=self.start_date,
                end_date=self.end_date
            )
        self.assertIn("Bad request", str(context.exception))

    def test_pipeline_with_invalid_dates(self):
        """Test pipeline behavior with invalid dates."""
        with self.assertRaises(ValueError) as context:
            self.handler.process_historical_data(
                latitude=self.test_latitude,
                longitude=self.test_longitude,
                start_date="invalid-date",
                end_date=self.end_date
            )
        self.assertIn("YYYY-MM-DD format", str(context.exception))

    @mock.patch("requests.get")
    @mock.patch("src.service.weather_api.WeatherSourceAPI")
    def test_pipeline_with_future_historical_dates(self, mock_api, mock_get):
        """Test pipeline behavior with future dates for historical data."""
        future_date = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Mock requests to raise error
        mock_response = mock.Mock()
        mock_response.status_code = 400
        mock_response.json.return_value = {"message": "Cannot fetch historical data for future dates"}
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "400 Client Error: Bad Request for url: test_url",
            response=mock_response
        )
        mock_get.return_value = mock_response
        
        with self.assertRaises(ValueError) as context:
            self.handler.process_historical_data(
                latitude=self.test_latitude,
                longitude=self.test_longitude,
                start_date=self.start_date,
                end_date=future_date
            )
        self.assertIn("Bad request", str(context.exception))

    @mock.patch("requests.get")
    def test_pipeline_with_invalid_file_format(self, mock_get):
        """Test pipeline behavior with invalid file format."""
        # Mock requests response to prevent actual API call
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "history": [
                {
                    "timestamp": "2023-12-01T00:00:00Z",
                    "temp": 20.5
                }
            ],
            "location": {
                "latitude": self.test_latitude,
                "longitude": self.test_longitude,
                "timezone": "America/New_York",
                "elevation": 10,
                "countryCode": "US",
                "countryName": "United States"
            }
        }
        mock_get.return_value = mock_response
        
        with self.assertRaises(ValueError) as context:
            self.handler.process_historical_data(
                latitude=self.test_latitude,
                longitude=self.test_longitude,
                start_date=self.start_date,
                end_date=self.end_date,
                file_format="invalid"
            )
        self.assertIn("Unsupported file format", str(context.exception))

    @mock.patch("requests.get")
    @mock.patch("src.service.weather_api.WeatherSourceAPI")
    @mock.patch("src.parser.weather_parser.WeatherDataParser")
    @mock.patch("boto3.client")
    def test_s3_storage_pipeline(self, mock_boto3, mock_parser, mock_api, mock_get):
        """Test pipeline with S3 storage."""
        # Initialize handler with S3 storage
        handler = WeatherDataHandler(use_s3=True)
        
        # Mock requests response
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "history": [
                {
                    "timestamp": "2023-12-01T00:00:00Z",
                    "temp": 20.5
                }
            ],
            "location": {
                "latitude": self.test_latitude,
                "longitude": self.test_longitude,
                "timezone": "America/New_York",
                "elevation": 10,
                "countryCode": "US",
                "countryName": "United States"
            }
        }
        mock_get.return_value = mock_response
        
        # Mock API to use our mocked response
        mock_api.return_value.get_historical_weather.return_value = mock_response.json()
        
        # Mock parser response
        mock_df = pd.DataFrame({
            "timestamp": ["2023-12-01T00:00:00Z"],
            "temp": [20.5],
            "latitude": [self.test_latitude],
            "longitude": [self.test_longitude],
            "timezone": ["America/New_York"],
            "data_type": ["historical"]
        })
        mock_parser.return_value.parse_historical_data.return_value = mock_df
        
        # Mock S3 client
        mock_s3 = mock_boto3.return_value
        
        file_path = handler.process_historical_data(
            latitude=self.test_latitude,
            longitude=self.test_longitude,
            start_date=self.start_date,
            end_date=self.end_date,
            file_format="parquet"
        )
        
        # Verify S3 upload was called
        self.assertTrue(file_path.startswith("s3://"))
        mock_s3.put_object.assert_called_once()

    @classmethod
    def tearDownClass(cls):
        """Clean up temporary files and directories."""
        import shutil
        shutil.rmtree(cls.temp_dir, ignore_errors=True)