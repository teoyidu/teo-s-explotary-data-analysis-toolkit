"""
Processor for handling data relationships
"""

import logging
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

class RelationshipsProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.relationships = config.get('relationships', [])
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to handle data relationships
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.relationships:
            return df
            
        for relationship in self.relationships:
            relationship_type = relationship.get('type')
            if relationship_type == 'foreign_key':
                df = self._handle_foreign_key(df, relationship)
            elif relationship_type == 'parent_child':
                df = self._handle_parent_child(df, relationship)
            elif relationship_type == 'many_to_many':
                df = self._handle_many_to_many(df, relationship)
                
        return df
        
    def _handle_foreign_key(self, df: pd.DataFrame, relationship: Dict) -> pd.DataFrame:
        """Handle foreign key relationships"""
        source_column = relationship.get('source_column')
        target_column = relationship.get('target_column')
        target_values = relationship.get('target_values', [])
        
        if not all([source_column, target_column, target_values]):
            return df
            
        # Check for invalid foreign keys
        mask = df[source_column].isin(target_values)
        if not mask.all():
            logger.warning(f"Found {(~mask).sum()} rows with invalid foreign keys in column {source_column}")
            if relationship.get('action') == 'remove':
                df = df[mask]
                
        return df
        
    def _handle_parent_child(self, df: pd.DataFrame, relationship: Dict) -> pd.DataFrame:
        """Handle parent-child relationships"""
        parent_column = relationship.get('parent_column')
        child_column = relationship.get('child_column')
        
        if not all([parent_column, child_column]):
            return df
            
        # Check for orphaned children
        parent_values = df[parent_column].unique()
        mask = df[child_column].isin(parent_values)
        if not mask.all():
            logger.warning(f"Found {(~mask).sum()} orphaned child records in column {child_column}")
            if relationship.get('action') == 'remove':
                df = df[mask]
                
        return df
        
    def _handle_many_to_many(self, df: pd.DataFrame, relationship: Dict) -> pd.DataFrame:
        """Handle many-to-many relationships"""
        source_column = relationship.get('source_column')
        target_column = relationship.get('target_column')
        
        if not all([source_column, target_column]):
            return df
            
        # Check for invalid relationships
        if 'valid_combinations' in relationship:
            valid_combinations = set(tuple(x) for x in relationship['valid_combinations'])
            actual_combinations = set(zip(df[source_column], df[target_column]))
            invalid_combinations = actual_combinations - valid_combinations
            
            if invalid_combinations:
                logger.warning(f"Found {len(invalid_combinations)} invalid many-to-many relationships")
                if relationship.get('action') == 'remove':
                    mask = df.apply(lambda row: (row[source_column], row[target_column]) in valid_combinations, axis=1)
                    df = df[mask]
                    
        return df 