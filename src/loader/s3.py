import os
import logging
from datetime import datetime
from io import BytesIO
from typing import List

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.helper.utils import getenv_or_raise, get_s3_client

# Set up module logger
logger = logging.getLogger(__name__)

class S3Loader:
    """AWS S3 data loader."""
    
    def __init__(self):
        """Initialize S3 loader with AWS credentials."""
        self.bucket_name = getenv_or_raise("S3_BUCKET_NAME")
        self.s3_client = get_s3_client(getenv_or_raise("AWS_REGION"))
    
    def _build_s3_path(self, folder: str, filename: str) -> str:
        """Build S3 path with proper forward slashes."""
        if folder:
            return f"{folder}/{filename}"
        return filename

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        filename_prefix: str,
        file_format: str = "parquet",
        folder: str = None
    ) -> str:
        """
        Save DataFrame to S3.
        
        Args:
            df: DataFrame to save
            filename_prefix: Prefix for the output filename
            file_format: Output file format (parquet or csv)
            folder: Optional folder path within the bucket
            
        Returns:
            S3 URI of saved file
        """
        try:
            # Create timestamp for unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename_prefix}_{timestamp}"
            
            # Create buffer for file data
            buffer = BytesIO()
            
            # Write to buffer in specified format
            if file_format.lower() == "parquet":
                file_path = f"{filename}.parquet"
                df.to_parquet(
                    buffer,
                    index=False,
                    engine='pyarrow'
                )
            elif file_format.lower() == "csv":
                file_path = f"{filename}.csv"
                df.to_csv(buffer, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            # Build S3 path with proper forward slashes
            key = self._build_s3_path(folder, file_path)
            
            # Upload to S3
            buffer.seek(0)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=buffer.getvalue()
            )
            
            # Create S3 URI
            s3_uri = f"s3://{self.bucket_name}/{key}"
            logger.info(f"Data saved to: {s3_uri}")
            return s3_uri
            
        except Exception as e:
            logger.error(f"Error saving data to S3: {str(e)}")
            raise
    
    def check_file_exists(self, key: str, folder: str = None) -> bool:
        """
        Check if a file exists in the S3 bucket.
        
        Args:
            key: S3 object key to check
            folder: Optional folder path within the bucket
            
        Returns:
            True if file exists, False otherwise
        """
        full_key = self._build_s3_path(folder, key)
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=full_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise
    
    def list_files(
        self,
        prefix: str = "",
        folder: str = None
    ) -> List[str]:
        """
        List files in the S3 bucket with given prefix.
        
        Args:
            prefix: Prefix to filter files (default: "")
            folder: Optional folder path to list files from
            
        Returns:
            List of file keys
        """
        try:
            # Combine folder with prefix using forward slashes
            full_prefix = self._build_s3_path(folder, prefix)
            
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=full_prefix
            )
            if 'Contents' not in response:
                return []
            return [obj['Key'] for obj in response['Contents']]
        except Exception as e:
            logger.error(f"Error listing files in S3: {str(e)}")
            raise
    
    # Alias for backward compatibility
    save_data = upload_dataframe