"""
Processor for cleaning Hadoop-specific tags and metadata from text data
"""

import logging
from typing import List, Dict, Any, Set, Optional
import pandas as pd
import re
from datetime import datetime
import json

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
        
        # Common Hadoop metadata patterns
        self.metadata_patterns = {
            'job_id': r'job_\d+_\d+',
            'task_id': r'task_\d+_\d+_\d+',
            'attempt_id': r'attempt_\d+_\d+_\d+_\d+',
            'container_id': r'container_\d+_\d+_\d+_\d+',
            'application_id': r'application_\d+_\d+',
            'executor_id': r'executor_\d+',
            'stage_id': r'stage_\d+',
            'partition_id': r'partition_\d+'
        }
        
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
            
            # Extract metadata if requested
            if settings.get('extract_metadata', False):
                df = self._extract_metadata(df, column, settings)
            
        return df
        
    def _clean_hadoop_tags(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean Hadoop-specific tags from text data"""
        def clean_text(text):
            try:
                cleaned_text = text
                
                # Get custom patterns if specified
                patterns = settings.get('custom_patterns', []) + self.hadoop_patterns
                
                # Remove Hadoop XML tags
                for pattern in patterns:
                    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.DOTALL)
                
                # Remove Hadoop-specific metadata if specified
                if settings.get('remove_metadata', True):
                    for pattern in self.metadata_patterns.values():
                        cleaned_text = re.sub(pattern, '', cleaned_text)
                
                # Preserve structured data if specified
                if settings.get('preserve_structured_data', False):
                    # Try to parse and preserve JSON/XML structures
                    try:
                        # Check if it's JSON
                        if cleaned_text.strip().startswith('{') or cleaned_text.strip().startswith('['):
                            data = json.loads(cleaned_text)
                            # Keep only specified fields
                            if 'preserve_fields' in settings:
                                data = {k: v for k, v in data.items() if k in settings['preserve_fields']}
                            cleaned_text = json.dumps(data)
                    except json.JSONDecodeError:
                        pass
                
                # Remove extra whitespace
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning Hadoop tags in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df
        
    def _extract_metadata(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Extract Hadoop metadata into separate columns"""
        try:
            # Get metadata fields to extract
            metadata_fields = settings.get('metadata_fields', list(self.metadata_patterns.keys()))
            
            for field in metadata_fields:
                if field in self.metadata_patterns:
                    pattern = self.metadata_patterns[field]
                    new_column = f"{column}_{field}"
                    
                    # Extract metadata using regex
                    df[new_column] = df[column].str.extract(pattern, expand=False)
                    
                    # Apply transformations if specified
                    if 'metadata_transformations' in settings and field in settings['metadata_transformations']:
                        transform = settings['metadata_transformations'][field]
                        if transform.get('type') == 'datetime':
                            df[new_column] = pd.to_datetime(df[new_column], format=transform.get('format'))
                        elif transform.get('type') == 'numeric':
                            df[new_column] = pd.to_numeric(df[new_column], errors='coerce')
            
            return df
        except Exception as e:
            logger.warning(f"Error extracting metadata from column {column}: {str(e)}")
            return df 