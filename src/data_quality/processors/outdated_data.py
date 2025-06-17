"""
Processor for handling outdated data
"""

import logging
from typing import List, Dict, Any
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class OutdatedDataProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.date_columns = config.get('date_columns', [])
        self.data_retention_days = config.get('data_retention_days', 365)
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to handle outdated data
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.date_columns:
            return df
            
        cutoff_date = datetime.now() - timedelta(days=self.data_retention_days)
        
        for col in self.date_columns:
            if col not in df.columns:
                continue
                
            # Convert to datetime if not already
            df[col] = pd.to_datetime(df[col], errors='coerce')
            
            # Remove rows with dates older than retention period
            mask = df[col] < cutoff_date
            if mask.any():
                logger.warning(f"Found {mask.sum()} rows with outdated data in column {col}")
                df = df[~mask]
                
        return df 