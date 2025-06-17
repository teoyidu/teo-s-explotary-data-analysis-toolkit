"""
Processor for cleaning Hadoop-specific tags and metadata from text data
"""

import logging
from typing import List, Dict, Any
import pandas as pd
import re

logger = logging.getLogger(__name__)

class HadoopCleanerProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.hadoop_columns = config.get('hadoop_columns', {})
        
        # Common Hadoop patterns
        self.hadoop_patterns = [
            r'<configuration>.*?</configuration>',  # Hadoop configuration blocks
            r'<property>.*?</property>',  # Property blocks
            r'<name>.*?</name>',  # Name tags
            r'<value>.*?</value>',  # Value tags
            r'<description>.*?</description>',  # Description tags
            r'<final>.*?</final>',  # Final tags
            r'<source>.*?</source>',  # Source tags
            r'<location>.*?</location>',  # Location tags
            r'<version>.*?</version>',  # Version tags
            r'<timestamp>.*?</timestamp>',  # Timestamp tags
            r'<job-id>.*?</job-id>',  # Job ID tags
            r'<task-id>.*?</task-id>',  # Task ID tags
            r'<attempt-id>.*?</attempt-id>',  # Attempt ID tags
            r'<status>.*?</status>',  # Status tags
            r'<progress>.*?</progress>',  # Progress tags
            r'<counters>.*?</counters>',  # Counters tags
            r'<counter-group>.*?</counter-group>',  # Counter group tags
            r'<counter>.*?</counter>',  # Counter tags
        ]
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to clean Hadoop tags
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.hadoop_columns:
            return df
            
        for column, settings in self.hadoop_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply Hadoop tag cleaning
            df = self._clean_hadoop_tags(df, column, settings)
            
        return df
        
    def _clean_hadoop_tags(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean Hadoop-specific tags from text data"""
        def clean_text(text):
            try:
                cleaned_text = text
                
                # Remove Hadoop XML tags
                for pattern in self.hadoop_patterns:
                    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.DOTALL)
                
                # Remove Hadoop-specific metadata if specified
                if settings.get('remove_metadata', True):
                    # Remove job IDs
                    cleaned_text = re.sub(r'job_\d+_\d+', '', cleaned_text)
                    # Remove task IDs
                    cleaned_text = re.sub(r'task_\d+_\d+_\d+', '', cleaned_text)
                    # Remove attempt IDs
                    cleaned_text = re.sub(r'attempt_\d+_\d+_\d+_\d+', '', cleaned_text)
                    # Remove container IDs
                    cleaned_text = re.sub(r'container_\d+_\d+_\d+_\d+', '', cleaned_text)
                
                # Remove extra whitespace
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning Hadoop tags in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df 