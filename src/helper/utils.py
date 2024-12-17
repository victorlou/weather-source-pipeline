import logging
import os
from datetime import datetime
from typing import Any, Dict

import boto3

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def getenv_or_raise(var_name: str) -> str:
    """
    Get environment variable or raise an error if it's not set.
    
    Args:
        var_name: Name of the environment variable
    
    Returns:
        Value of the environment variable
    
    Raises:
        ValueError: If the environment variable is not set
    """
    value = os.getenv(var_name)
    if value is None:
        raise ValueError(f"Missing required environment variable: {var_name}")
    return value

def configure_logging():
    """Configure root logger with common settings."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getenv_or_raise("LOG_LEVEL"))
    
    if not root_logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

# Configure logging at module import
configure_logging()

def create_directory_if_not_exists(directory_path: str) -> None:
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def validate_date_format(date_str: str) -> bool:
    """Validate if a string is in YYYY-MM-DD format."""
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def format_api_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """Format API response to a standard structure."""
    return {
        "timestamp": datetime.now().isoformat(),
        "status": response.get("status", "unknown"),
        "data": response.get("data", {}),
        "metadata": {
            "source": "weather_source",
            "version": "2.0.0"
        }
    }

def get_s3_client(region_name: str) -> boto3.client:
    """
    Get S3 client with either environment credentials or IAM role.
    
    Args:
        region_name: AWS region name
    
    Returns:
        Configured S3 client
    """
    # Try to get explicit credentials
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    # If credentials are provided, use them
    if aws_access_key and aws_secret_key:
        return boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
    
    # Otherwise, let boto3 use its credential chain (IAM role)
    return boto3.client('s3', region_name=region_name)

logger = logging.getLogger(__name__) 