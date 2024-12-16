import datetime
from unittest import TestCase

import pandas as pd
import pytest

from src.parser.weather_parser import WeatherDataParser


class TestWeatherDataParser(TestCase):
    def setUp(self):
        self.parser = WeatherDataParser()
        self.sample_historical_data = {
            "history": [
                {
                    "timestamp": "2023-12-01T00:00:00Z",
                    "temp": 20.5,
                    "precip": 0.0,
                    "relHum": 65
                }
            ],
            "location": {
                "latitude": 40.7128,
                "longitude": -74.0060,
                "timezone": "America/New_York",
                "elevation": 10,
                "countryCode": "US",
                "countryName": "United States"
            }
        }
        self.sample_forecast_data = {
            "forecast": [
                {
                    "timestamp": "2023-12-15T00:00:00Z",
                    "temp": 18.3,
                    "precip": 0.2,
                    "precipProb": 30
                }
            ],
            "location": {
                "latitude": 40.7128,
                "longitude": -74.0060,
                "timezone": "America/New_York",
                "elevation": 10,
                "countryCode": "US",
                "countryName": "United States"
            }
        }

    def test_parse_historical_data_success(self):
        df = self.parser.parse_historical_data(self.sample_historical_data)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["data_type"].iloc[0], "historical")
        self.assertEqual(df["temp"].iloc[0], 20.5)
        self.assertEqual(df["latitude"].iloc[0], 40.7128)
        self.assertEqual(df["longitude"].iloc[0], -74.0060)
        self.assertEqual(df["timezone"].iloc[0], "America/New_York")

    def test_parse_historical_data_empty(self):
        df = self.parser.parse_historical_data({"history": []})
        self.assertTrue(df.empty)

    def test_parse_forecast_data_success(self):
        df = self.parser.parse_forecast_data(self.sample_forecast_data)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df["data_type"].iloc[0], "forecast")
        self.assertEqual(df["temp"].iloc[0], 18.3)
        self.assertEqual(df["precip"].iloc[0], 0.2)
        self.assertEqual(df["precipProb"].iloc[0], 30)
        self.assertEqual(df["latitude"].iloc[0], 40.7128)
        self.assertEqual(df["longitude"].iloc[0], -74.0060)

    def test_parse_forecast_data_empty(self):
        df = self.parser.parse_forecast_data({"forecast": []})
        self.assertTrue(df.empty)

    def test_validate_data_success(self):
        df = pd.DataFrame({
            "latitude": [40.7128],
            "longitude": [-74.0060],
            "timestamp": [datetime.datetime.now()],
            "data_type": ["historical"],
            "processed_at": [datetime.datetime.now()]
        })
        self.assertTrue(self.parser.validate_data(df))

    def test_validate_data_missing_columns(self):
        df = pd.DataFrame({
            "latitude": [40.7128],
            "longitude": [-74.0060]
        })
        self.assertFalse(self.parser.validate_data(df))

    def test_validate_data_null_values(self):
        df = pd.DataFrame({
            "latitude": [None],
            "longitude": [-74.0060],
            "timestamp": [datetime.datetime.now()],
            "data_type": ["historical"],
            "processed_at": [datetime.datetime.now()]
        })
        self.assertFalse(self.parser.validate_data(df))

    def test_validate_data_empty_df(self):
        df = pd.DataFrame()
        self.assertFalse(self.parser.validate_data(df)) 