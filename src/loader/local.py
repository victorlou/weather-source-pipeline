import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

# Set up module logger
logger = logging.getLogger(__name__)


class LocalLoader:
    """Local file system data loader."""
    
    def __init__(self, output_dir: str = "data"):
        """
        Initialize local loader.
        
        Args:
            output_dir: Directory to save files (default: data)
        """
        self.output_dir = output_dir
    
    def save_dataframe(
        self,
        df: pd.DataFrame,
        filename_prefix: str,
        file_format: str = "parquet"
    ) -> str:
        """
        Save DataFrame to local file system.
        
        Args:
            df: DataFrame to save
            filename_prefix: Prefix for the output filename
            file_format: Output file format (parquet or csv)
            
        Returns:
            Path to saved file
        """
        try:
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Create timestamp for unique filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{filename_prefix}_{timestamp}"
            
            # Create full path
            if file_format.lower() == "parquet":
                filepath = os.path.join(self.output_dir, f"{filename}.parquet")
                df.to_parquet(filepath, index=False)
            elif file_format.lower() == "csv":
                filepath = os.path.join(self.output_dir, f"{filename}.csv")
                df.to_csv(filepath, index=False)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
            
            logger.info(f"Data saved to: {filepath}")
            return filepath
            
        except Exception as e:
            logger.error(f"Error saving data to local file: {str(e)}")
            raise
    
    def check_file_exists(self, filename: str) -> bool:
        """
        Check if a file exists in the output directory.
        
        Args:
            filename: Name of the file to check
            
        Returns:
            True if file exists, False otherwise
        """
        filepath = Path(os.path.join(self.output_dir, filename))
        return filepath.exists()
    
    # Alias for backward compatibility
    save_data = save_dataframe