"""
Processor for handling data uniqueness
"""

import logging
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

class UniquenessProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.unique_constraints = config.get('unique_constraints', [])
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to handle uniqueness constraints
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.unique_constraints:
            return df
            
        for constraint in self.unique_constraints:
            columns = constraint.get('columns', [])
            if not columns:
                continue
                
            action = constraint.get('action', 'drop_duplicates')
            if action == 'drop_duplicates':
                df = df.drop_duplicates(subset=columns, keep='first')
            elif action == 'keep_last':
                df = df.drop_duplicates(subset=columns, keep='last')
            elif action == 'mark':
                df = self._mark_duplicates(df, columns)
                
        return df
        
    def _mark_duplicates(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """
        Mark duplicate rows with a flag
        
        Args:
            df (pd.DataFrame): Input DataFrame
            columns (List[str]): Columns to check for duplicates
            
        Returns:
            pd.DataFrame: Processed DataFrame with duplicate flag
        """
        # Create duplicate flag column
        df['is_duplicate'] = df.duplicated(subset=columns, keep='first')
        
        # Log duplicate count
        duplicate_count = df['is_duplicate'].sum()
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate rows based on columns {columns}")
            
        return df 