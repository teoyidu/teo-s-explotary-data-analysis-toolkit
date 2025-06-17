"""
XLSX file processor for data quality framework
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

class XLSXProcessor:
    def __init__(self, config: Dict):
        """
        Initialize XLSX processor with configuration
        
        Args:
            config (Dict): Configuration dictionary containing processing parameters
        """
        self.config = config
        self.supported_extensions = ['.xlsx', '.xls']
        
    def process_file(self, file_path: str) -> pd.DataFrame:
        """
        Process an XLSX file with filtering, deduplication, and decontamination
        
        Args:
            file_path (str): Path to the XLSX file
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        logger.info(f"Processing XLSX file: {file_path}")
        
        # Read the XLSX file
        df = pd.read_excel(file_path)
        
        # Apply data cleaning steps
        df = self._clean_data(df)
        
        # Apply filtering
        df = self._apply_filters(df)
        
        # Apply deduplication
        df = self._deduplicate(df)
        
        # Apply decontamination
        df = self._decontaminate(df)
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the data by handling missing values and data types"""
        # Handle missing values based on strategy
        if self.config.get('missing_value_strategy') == 'fill':
            fill_values = self.config.get('fill_values', {})
            df = df.fillna(fill_values)
        elif self.config.get('missing_value_strategy') == 'drop':
            df = df.dropna(subset=self.config.get('mandatory_fields', []))
            
        # Convert numerical columns
        for col in self.config.get('numerical_columns', []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        # Convert date columns
        for col in self.config.get('date_columns', []):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        return df
    
    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply configured filters to the DataFrame"""
        filters = self.config.get('filters', {})
        
        for column, filter_config in filters.items():
            if column not in df.columns:
                continue
                
            if 'min_value' in filter_config:
                df = df[df[column] >= filter_config['min_value']]
            if 'max_value' in filter_config:
                df = df[df[column] <= filter_config['max_value']]
            if 'allowed_values' in filter_config:
                df = df[df[column].isin(filter_config['allowed_values'])]
            if 'regex_pattern' in filter_config:
                df = df[df[column].str.match(filter_config['regex_pattern'], na=False)]
                
        return df
    
    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate rows based on configuration"""
        unique_constraints = self.config.get('unique_constraints', [])
        
        for constraint in unique_constraints:
            columns = constraint.get('columns', [])
            if not columns:
                continue
                
            action = constraint.get('action', 'drop_duplicates')
            if action == 'drop_duplicates':
                df = df.drop_duplicates(subset=columns, keep='first')
            elif action == 'keep_last':
                df = df.drop_duplicates(subset=columns, keep='last')
                
        return df
    
    def _decontaminate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize data values"""
        # Remove leading/trailing whitespace from string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip()
            
        # Convert to lowercase if specified
        if self.config.get('case_sensitive', False) is False:
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].str.lower()
                
        # Remove special characters if specified
        if self.config.get('remove_special_chars', False):
            for col in df.select_dtypes(include=['object']).columns:
                df[col] = df[col].str.replace(r'[^\w\s]', '', regex=True)
                
        return df
    
    def save_processed_file(self, df: pd.DataFrame, output_path: str) -> None:
        """
        Save the processed DataFrame to an XLSX file
        
        Args:
            df (pd.DataFrame): Processed DataFrame
            output_path (str): Path where to save the processed file
        """
        # Create output directory if it doesn't exist
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Save to XLSX
        df.to_excel(output_path, index=False)
        logger.info(f"Saved processed file to: {output_path}") 