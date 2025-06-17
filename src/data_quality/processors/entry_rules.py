"""
Processor for handling data entry rules
"""

import logging
from typing import List, Dict, Any, Optional
import pandas as pd

logger = logging.getLogger(__name__)

class EntryRulesProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.entry_rules = config.get('entry_rules', [])
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to apply entry rules
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.entry_rules:
            return df
            
        for rule in self.entry_rules:
            rule_type = rule.get('type')
            if rule_type == 'conditional':
                df = self._apply_conditional_rule(df, rule)
            elif rule_type == 'derived':
                df = self._apply_derived_rule(df, rule)
            elif rule_type == 'validation':
                df = self._apply_validation_rule(df, rule)
                
        return df
        
    def _apply_conditional_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> pd.DataFrame:
        """Apply conditional rules"""
        condition = rule.get('condition')
        action = rule.get('action', {})
        target_column = rule.get('target_column')
        
        if not all([condition, action, target_column]):
            logger.warning("Missing required parameters for conditional rule")
            return df
            
        try:
            # Apply condition
            mask = df.eval(str(condition))
            
            action_type = action.get('type')
            if action_type == 'set_value':
                df.loc[mask, target_column] = action.get('value')
            elif action_type == 'remove':
                df = df[~mask]
                
        except Exception as e:
            logger.error(f"Error applying conditional rule: {str(e)}")
            
        return df
        
    def _apply_derived_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> pd.DataFrame:
        """Apply derived value rules"""
        target_column = rule.get('target_column')
        expression = rule.get('expression')
        
        if not all([target_column, expression]):
            logger.warning("Missing required parameters for derived rule")
            return df
            
        try:
            # Calculate derived value
            df[target_column] = df.eval(str(expression))
        except Exception as e:
            logger.error(f"Error applying derived rule: {str(e)}")
            
        return df
        
    def _apply_validation_rule(self, df: pd.DataFrame, rule: Dict[str, Any]) -> pd.DataFrame:
        """Apply validation rules"""
        condition = rule.get('condition')
        action = rule.get('action', {})
        
        if not all([condition, action]):
            logger.warning("Missing required parameters for validation rule")
            return df
            
        try:
            # Apply validation
            mask = df.eval(str(condition))
            
            if not mask.all():
                logger.warning(f"Found {(~mask).sum()} rows violating validation rule")
                action_type = action.get('type')
                if action_type == 'remove':
                    df = df[mask]
                elif action_type == 'set_value':
                    target_column = action.get('target_column')
                    value = action.get('value')
                    if target_column and value is not None:
                        df.loc[~mask, target_column] = value
                        
        except Exception as e:
            logger.error(f"Error applying validation rule: {str(e)}")
            
        return df 