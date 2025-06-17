"""
Processor for handling mandatory fields validation
"""

import logging
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

class MandatoryFieldsProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.mandatory_fields = config.get('mandatory_fields', [])
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to validate mandatory fields
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.mandatory_fields:
            return df
            
        # Check for missing values in mandatory fields
        missing_mask = df[self.mandatory_fields].isna().any(axis=1)
        if missing_mask.any():
            logger.warning(f"Found {missing_mask.sum()} rows with missing mandatory fields")
            
        # Remove rows with missing mandatory fields
        df = df.dropna(subset=self.mandatory_fields)
        
        return df 