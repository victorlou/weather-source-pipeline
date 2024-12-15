import logging
import os
from datetime import datetime
from typing import Any, Dict

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

logger = logging.getLogger(__name__) 