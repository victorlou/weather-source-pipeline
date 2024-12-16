import os
from unittest import TestCase, mock

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from src.loader.local import LocalLoader
from src.loader.s3 import S3Loader


class TestLocalLoader(TestCase):
    def setUp(self):
        with mock.patch("os.getenv") as mock_getenv:
            mock_getenv.return_value = "./test_data"
            self.loader = LocalLoader()
        self.test_df = pd.DataFrame({
            "temperature": [20.5],
            "humidity": [65]
        })

    @mock.patch("pandas.DataFrame.to_parquet")
    @mock.patch("os.makedirs")
    def test_save_dataframe_parquet(self, mock_makedirs, mock_to_parquet):
        result = self.loader.save_dataframe(self.test_df, "historical", "parquet")
        
        self.assertIn("historical", result)
        self.assertIn(".parquet", result)
        mock_makedirs.assert_called()
        mock_to_parquet.assert_called_once()

    @mock.patch("pandas.DataFrame.to_csv")
    @mock.patch("os.makedirs")
    def test_save_dataframe_csv(self, mock_makedirs, mock_to_csv):
        result = self.loader.save_dataframe(self.test_df, "historical", "csv")
        
        self.assertIn("historical", result)
        self.assertIn(".csv", result)
        mock_makedirs.assert_called()
        mock_to_csv.assert_called_once()

    def test_save_dataframe_invalid_format(self):
        with self.assertRaises(ValueError):
            self.loader.save_dataframe(self.test_df, "historical", "invalid")

    @mock.patch("pathlib.Path.exists")
    def test_check_file_exists(self, mock_exists):
        mock_exists.return_value = True
        self.assertTrue(self.loader.check_file_exists("test.parquet"))

        mock_exists.return_value = False
        self.assertFalse(self.loader.check_file_exists("test.parquet"))


class TestS3Loader(TestCase):
    def setUp(self):
        with mock.patch("os.getenv") as mock_getenv, \
             mock.patch("boto3.client") as mock_client:
            mock_getenv.return_value = "test-bucket"
            self.loader = S3Loader()
            self.mock_s3 = mock_client.return_value
        
        self.test_df = pd.DataFrame({
            "temperature": [20.5],
            "humidity": [65]
        })

    @mock.patch("pandas.DataFrame.to_parquet")
    def test_upload_dataframe_parquet(self, mock_to_parquet):
        mock_to_parquet.return_value = b"parquet_data"
        
        result = self.loader.upload_dataframe(self.test_df, "historical", "parquet")
        
        self.assertIn("s3://", result)
        self.assertIn("historical", result)
        self.assertIn(".parquet", result)
        self.mock_s3.put_object.assert_called_once()

    @mock.patch("pandas.DataFrame.to_csv")
    def test_upload_dataframe_csv(self, mock_to_csv):
        mock_to_csv.return_value = "csv_data"
        
        result = self.loader.upload_dataframe(self.test_df, "historical", "csv")
        
        self.assertIn("s3://", result)
        self.assertIn("historical", result)
        self.assertIn(".csv", result)
        self.mock_s3.put_object.assert_called_once()

    def test_upload_dataframe_invalid_format(self):
        with self.assertRaises(ValueError):
            self.loader.upload_dataframe(self.test_df, "historical", "invalid")

    def test_check_file_exists_true(self):
        self.mock_s3.head_object.return_value = {}
        self.assertTrue(self.loader.check_file_exists("test.parquet"))

    def test_check_file_exists_false(self):
        self.mock_s3.head_object.side_effect = ClientError(
            {"Error": {"Code": "404"}}, "head_object"
        )
        self.assertFalse(self.loader.check_file_exists("test.parquet"))

    def test_list_files(self):
        self.mock_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "file1.parquet"},
                {"Key": "file2.parquet"}
            ]
        }
        
        result = self.loader.list_files("prefix")
        
        self.assertEqual(len(result), 2)
        self.mock_s3.list_objects_v2.assert_called_once()

    def test_list_files_empty(self):
        self.mock_s3.list_objects_v2.return_value = {}
        result = self.loader.list_files("prefix")
        self.assertEqual(result, []) 