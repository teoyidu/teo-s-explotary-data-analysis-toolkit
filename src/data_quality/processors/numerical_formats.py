"""
Processor for handling numerical data formats
"""

import logging
from typing import List, Dict, Any
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class NumericalFormatsProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.numerical_columns = config.get('numerical_columns', [])
        self.decimal_places = config.get('decimal_places', 2)
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to handle numerical formats
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.numerical_columns:
            return df
            
        for col in self.numerical_columns:
            if col not in df.columns:
                continue
                
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Round to specified decimal places
            df[col] = df[col].round(self.decimal_places)
            
            # Handle outliers if specified
            if self.config.get('handle_outliers', False):
                df = self._handle_outliers(df, col)
                
        return df
        
    def _handle_outliers(self, df: pd.DataFrame, column: str) -> pd.DataFrame:
        """
        Handle outliers in numerical columns
        
        Args:
            df (pd.DataFrame): Input DataFrame
            column (str): Column name to process
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        # Calculate IQR
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        
        # Define bounds
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Replace outliers with bounds
        df[column] = df[column].clip(lower=lower_bound, upper=upper_bound)
        
        return df 