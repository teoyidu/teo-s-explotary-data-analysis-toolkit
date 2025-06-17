"""
Processor for handling categorical data
"""

import logging
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

class CategoriesProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.category_columns = config.get('category_columns', {})
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to handle categorical data
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.category_columns:
            return df
            
        for column, settings in self.category_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply case sensitivity setting
            if not settings.get('case_sensitive', False):
                df[column] = df[column].str.lower()
                
            # Apply value mapping if specified
            if 'value_mapping' in settings:
                df[column] = df[column].map(settings['value_mapping']).fillna(settings.get('default_value', 'unknown'))
                
            # Apply allowed values if specified
            if 'allowed_values' in settings:
                mask = df[column].isin(settings['allowed_values'])
                if not mask.all():
                    logger.warning(f"Found {(~mask).sum()} rows with invalid categories in column {column}")
                    if settings.get('action') == 'remove':
                        df = df[mask]
                    else:
                        df.loc[~mask, column] = settings.get('default_value', 'unknown')
                        
        return df 