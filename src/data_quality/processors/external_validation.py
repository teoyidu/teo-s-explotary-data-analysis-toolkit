"""
Processor for external data validation
"""

import logging
from typing import List, Dict, Any
import pandas as pd

logger = logging.getLogger(__name__)

class ExternalValidationProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.validation_rules = config.get('validation_rules', {})
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to apply external validation rules
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.validation_rules:
            return df
            
        for column, rules in self.validation_rules.items():
            if column not in df.columns:
                continue
                
            for rule in rules:
                rule_type = rule.get('type')
                if rule_type == 'regex':
                    df = self._apply_regex_rule(df, column, rule)
                elif rule_type == 'lookup':
                    df = self._apply_lookup_rule(df, column, rule)
                elif rule_type == 'range':
                    df = self._apply_range_rule(df, column, rule)
                    
        return df
        
    def _apply_regex_rule(self, df: pd.DataFrame, column: str, rule: Dict) -> pd.DataFrame:
        """Apply regex validation rule"""
        pattern = rule.get('pattern')
        if not pattern:
            return df
            
        mask = df[column].str.match(pattern, na=False)
        if not mask.all():
            logger.warning(f"Found {(~mask).sum()} rows violating regex pattern in column {column}")
            
        if rule.get('action') == 'remove':
            df = df[mask]
            
        return df
        
    def _apply_lookup_rule(self, df: pd.DataFrame, column: str, rule: Dict) -> pd.DataFrame:
        """Apply lookup validation rule"""
        valid_values = rule.get('valid_values', [])
        if not valid_values:
            return df
            
        mask = df[column].isin(valid_values)
        if not mask.all():
            logger.warning(f"Found {(~mask).sum()} rows with invalid values in column {column}")
            
        if rule.get('action') == 'remove':
            df = df[mask]
            
        return df
        
    def _apply_range_rule(self, df: pd.DataFrame, column: str, rule: Dict) -> pd.DataFrame:
        """Apply range validation rule"""
        min_value = rule.get('min_value')
        max_value = rule.get('max_value')
        
        if min_value is not None:
            mask = df[column] >= min_value
            if not mask.all():
                logger.warning(f"Found {(~mask).sum()} rows below minimum value in column {column}")
            df = df[mask]
            
        if max_value is not None:
            mask = df[column] <= max_value
            if not mask.all():
                logger.warning(f"Found {(~mask).sum()} rows above maximum value in column {column}")
            df = df[mask]
            
        return df 