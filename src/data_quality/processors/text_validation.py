"""
Processor for text data validation
"""

import logging
from typing import List, Dict, Any
import pandas as pd
import re

logger = logging.getLogger(__name__)

class TextValidationProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.text_columns = config.get('text_columns', {})
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to validate text data
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.text_columns:
            return df
            
        for column, settings in self.text_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply text cleaning
            df = self._clean_text(df, column, settings)
            
            # Apply validation rules
            df = self._validate_text(df, column, settings)
            
        return df
        
    def _clean_text(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean text data"""
        # Remove leading/trailing whitespace
        df[column] = df[column].str.strip()
        
        # Remove extra whitespace
        df[column] = df[column].str.replace(r'\s+', ' ', regex=True)
        
        # Apply case transformation
        if settings.get('case') == 'lower':
            df[column] = df[column].str.lower()
        elif settings.get('case') == 'upper':
            df[column] = df[column].str.upper()
        elif settings.get('case') == 'title':
            df[column] = df[column].str.title()
            
        # Remove special characters if specified
        if settings.get('remove_special_chars', False):
            df[column] = df[column].str.replace(r'[^\w\s]', '', regex=True)
            
        return df
        
    def _validate_text(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Validate text data"""
        # Check minimum length
        if 'min_length' in settings:
            mask = df[column].str.len() >= settings['min_length']
            if not mask.all():
                logger.warning(f"Found {(~mask).sum()} rows with text shorter than minimum length in column {column}")
                if settings.get('action') == 'remove':
                    df = df[mask]
                    
        # Check maximum length
        if 'max_length' in settings:
            mask = df[column].str.len() <= settings['max_length']
            if not mask.all():
                logger.warning(f"Found {(~mask).sum()} rows with text longer than maximum length in column {column}")
                if settings.get('action') == 'remove':
                    df = df[mask]
                    
        # Check regex pattern
        if 'pattern' in settings:
            mask = df[column].str.match(settings['pattern'], na=False)
            if not mask.all():
                logger.warning(f"Found {(~mask).sum()} rows not matching pattern in column {column}")
                if settings.get('action') == 'remove':
                    df = df[mask]
                    
        return df 