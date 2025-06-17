"""
Processor for cleaning boilerplate text from data
"""

import logging
from typing import List, Dict, Any
import pandas as pd
import re
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

class BoilerplateCleanerProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.boilerplate_columns = config.get('boilerplate_columns', {})
        
        # Common boilerplate patterns
        self.common_patterns = [
            r'^\s*Copyright.*?$',  # Copyright notices
            r'^\s*All rights reserved.*?$',  # Rights reserved notices
            r'^\s*Confidential.*?$',  # Confidentiality notices
            r'^\s*Proprietary.*?$',  # Proprietary notices
            r'^\s*Page \d+ of \d+.*?$',  # Page numbers
            r'^\s*Generated on.*?$',  # Generation timestamps
            r'^\s*Last updated.*?$',  # Last updated timestamps
            r'^\s*Version \d+\.\d+.*?$',  # Version numbers
            r'^\s*Document ID:.*?$',  # Document IDs
            r'^\s*Reference:.*?$',  # Reference numbers
        ]
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to clean boilerplate text
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.boilerplate_columns:
            return df
            
        for column, settings in self.boilerplate_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply boilerplate cleaning
            df = self._clean_boilerplate(df, column, settings)
            
        return df
        
    def _clean_boilerplate(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean boilerplate text from data"""
        def clean_text(text):
            try:
                cleaned_text = text
                
                # Remove common boilerplate patterns
                for pattern in self.common_patterns:
                    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.MULTILINE)
                
                # Remove custom boilerplate patterns if specified
                if 'custom_patterns' in settings:
                    for pattern in settings['custom_patterns']:
                        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.MULTILINE)
                
                # Remove duplicate content if specified
                if settings.get('remove_duplicates', False):
                    lines = cleaned_text.split('\n')
                    unique_lines = []
                    for line in lines:
                        if not any(SequenceMatcher(None, line, existing).ratio() > 0.9 
                                 for existing in unique_lines):
                            unique_lines.append(line)
                    cleaned_text = '\n'.join(unique_lines)
                
                # Remove header/footer if specified
                if settings.get('remove_header_footer', False):
                    lines = cleaned_text.split('\n')
                    if len(lines) > 4:  # Only process if there are enough lines
                        # Remove first and last 2 lines (typical header/footer)
                        cleaned_text = '\n'.join(lines[2:-2])
                
                # Remove extra whitespace
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning boilerplate in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df 