"""
Processor for cleaning HTML tags from text data
"""

import logging
from typing import List, Dict, Any
import pandas as pd
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)

class HTMLCleanerProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.html_columns = config.get('html_columns', {})
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to clean HTML tags
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.html_columns:
            return df
            
        for column, settings in self.html_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply HTML cleaning
            df = self._clean_html(df, column, settings)
            
        return df
        
    def _clean_html(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean HTML tags from text data"""
        def clean_text(text):
            try:
                # Parse HTML
                soup = BeautifulSoup(text, 'html.parser')
                
                # Get text content
                cleaned_text = soup.get_text(separator=' ', strip=True)
                
                # Remove extra whitespace
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                
                # Handle specific HTML entities if needed
                if settings.get('handle_entities', True):
                    cleaned_text = cleaned_text.replace('&nbsp;', ' ')
                    cleaned_text = cleaned_text.replace('&amp;', '&')
                    cleaned_text = cleaned_text.replace('&lt;', '<')
                    cleaned_text = cleaned_text.replace('&gt;', '>')
                
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning HTML in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df 