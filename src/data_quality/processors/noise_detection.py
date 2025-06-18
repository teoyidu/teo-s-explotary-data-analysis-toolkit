"""
Processor for detecting and handling noise in text data, particularly from OCR sources
"""

import logging
from typing import List, Dict, Any
import pandas as pd
import re
from collections import Counter

logger = logging.getLogger(__name__)

class NoiseDetectionProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.text_columns = config.get('text_columns', {})
        self.min_alpha_ratio = config.get('min_alpha_ratio', 0.7)
        self.max_bad_char_ratio = config.get('max_bad_char_ratio', 0.3)
        self.bad_char_pattern = config.get('bad_char_pattern', r'[^a-zA-Z0-9\s.,!?-]')
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to detect and handle noise in text data
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame with noise detection results
        """
        if not self.text_columns:
            return df
            
        for column, settings in self.text_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Add noise detection columns
            df[f'{column}_alpha_ratio'] = df[column].apply(self._calculate_alpha_ratio)
            df[f'{column}_bad_char_ratio'] = df[column].apply(self._calculate_bad_char_ratio)
            
            # Apply noise filtering if configured
            if settings.get('filter_noise', False):
                df = self._filter_noisy_rows(df, column, settings)
                
        return df
        
    def _calculate_alpha_ratio(self, text: str) -> float:
        """
        Calculate the ratio of alphabetic characters in the text
        
        Args:
            text (str): Input text
            
        Returns:
            float: Ratio of alphabetic characters
        """
        if not text:
            return 0.0
            
        alpha_count = sum(c.isalpha() for c in text)
        return alpha_count / len(text)
        
    def _calculate_bad_char_ratio(self, text: str) -> float:
        """
        Calculate the ratio of bad characters in the text
        
        Args:
            text (str): Input text
            
        Returns:
            float: Ratio of bad characters
        """
        if not text:
            return 0.0
            
        bad_chars = len(re.findall(self.bad_char_pattern, text))
        return bad_chars / len(text)
        
    def _filter_noisy_rows(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """
        Filter out rows with excessive noise
        
        Args:
            df (pd.DataFrame): Input DataFrame
            column (str): Column name
            settings (Dict): Column settings
            
        Returns:
            pd.DataFrame: Filtered DataFrame
        """
        min_alpha = settings.get('min_alpha_ratio', self.min_alpha_ratio)
        max_bad = settings.get('max_bad_char_ratio', self.max_bad_char_ratio)
        
        # Create mask for valid rows
        valid_mask = (
            (df[f'{column}_alpha_ratio'] >= min_alpha) &
            (df[f'{column}_bad_char_ratio'] <= max_bad)
        )
        
        # Log statistics
        noisy_count = (~valid_mask).sum()
        if noisy_count > 0:
            logger.warning(
                f"Found {noisy_count} noisy rows in column {column} "
                f"(alpha_ratio < {min_alpha} or bad_char_ratio > {max_bad})"
            )
            
        # Apply filtering if configured
        if settings.get('action') == 'remove':
            df = df[valid_mask]
            
        return df 