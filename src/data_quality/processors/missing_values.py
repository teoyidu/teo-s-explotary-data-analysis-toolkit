"""
Processor for handling missing values in data
"""

import logging
from typing import Dict, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan, isnull

from ..core.exceptions import ValidationError

logger = logging.getLogger(__name__)

class MissingValuesProcessor:
    """Processor for handling missing values in data"""
    
    def __init__(self, config: Dict):
        """
        Initialize the processor
        
        Args:
            config: Configuration dictionary containing missing value handling rules
        """
        self.config = config
        self.missing_value_strategy = config.get('missing_value_strategy', 'drop')
        self.missing_threshold = config.get('missing_threshold', 50.0)
        self.critical_columns = config.get('critical_columns', [])
        self.fill_values = config.get('fill_values', {})
    
    def process(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """
        Process the DataFrame to handle missing values
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (processed DataFrame, statistics dictionary)
        """
        stats = {}
        
        try:
            # Calculate missing value statistics
            total_rows = df.count()
            
            for column in df.columns:
                missing_count = df.filter(col(column).isNull() | isnan(col(column))).count()
                missing_percentage = (missing_count / total_rows) * 100 if total_rows > 0 else 0
                
                stats[column] = {
                    'missing_count': missing_count,
                    'missing_percentage': missing_percentage
                }
                
                logger.info(f"Column {column}: {missing_count} missing values ({missing_percentage:.2f}%)")
            
            # Handle missing values based on configuration
            if self.missing_value_strategy == 'drop':
                # Drop rows with missing values in critical columns
                if self.critical_columns:
                    df_cleaned = df.dropna(subset=self.critical_columns)
                else:
                    df_cleaned = df.dropna()
            elif self.missing_value_strategy == 'fill':
                # Fill missing values with defaults
                df_cleaned = df.fillna(self.fill_values)
            else:
                df_cleaned = df
            
            stats['rows_dropped'] = total_rows - df_cleaned.count()
            
        except Exception as e:
            logger.error(f"Error in missing values processing: {str(e)}")
            raise ValidationError(f"Failed to process missing values: {str(e)}")
        
        return df_cleaned, stats 