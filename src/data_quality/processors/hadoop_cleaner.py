"""
Processor for cleaning Hadoop-specific tags and metadata from text data
"""

import logging
from typing import List, Dict, Any, Set, Optional
import pandas as pd
import re
from datetime import datetime
import json
import xml.etree.ElementTree as ET

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
        # Common Hadoop metadata patterns (with capture groups)
        self.metadata_patterns = {
            'job_id': r'(job_\d+_\d+)',
            'task_id': r'(task_\d+_\d+_\d+)',
            'attempt_id': r'(attempt_\d+_\d+_\d+_\d+)',
            'container_id': r'(container_\d+_\d+_\d+_\d+)',
            'application_id': r'(application_\d+_\d+)',
            'executor_id': r'(executor_\d+)',
            'stage_id': r'(stage_\d+)',
            'partition_id': r'(partition_\d+)'
        }
        # For generic XML tag removal
        self.xml_tag_pattern = r'<[^>]+>'
        
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
        
        # Create a copy once for all metadata extraction
        original_df = df.copy(deep=True)
        
        # Extract metadata first for all columns that need it
        for column, settings in self.hadoop_columns.items():
            if column not in df.columns:
                continue
            
            if settings.get('extract_metadata', False):
                df = self._extract_metadata(df, column, settings, original_df=original_df)
        
        # Then clean tags for all columns
        for column, settings in self.hadoop_columns.items():
            if column not in df.columns:
                continue
            
            # Convert to string and clean tags
            df[column] = df[column].astype(str)
            df = self._clean_hadoop_tags(df, column, settings)
        return df
    
    def _extract_value_tags(self, text: str) -> str:
        """Extract only the values inside <value> tags. If none, return all text content."""
        try:
            # Try to parse as XML
            root = ET.fromstring(f'<root>{text}</root>')
            values = [elem.text for elem in root.iter('value') if elem.text]
            if values:
                return ' '.join(values)
            # If no <value> tags, return all text content (concatenated)
            all_text = ''.join(root.itertext()).strip()
            return all_text
        except Exception:
            # If not XML, fallback to removing tags
            return re.sub(self.xml_tag_pattern, '', text)
    
    def _clean_hadoop_tags(self, df: pd.DataFrame, column: str, settings: Dict[str, Any]) -> pd.DataFrame:
        """Clean Hadoop-specific tags from text data, preserving only <value> tag content if present."""
        def clean_text(text):
            try:
                cleaned_text = text
                # Extract only <value> tag content if present, else all text
                cleaned_text = self._extract_value_tags(cleaned_text)
                # Remove custom patterns if specified
                patterns = settings.get('custom_patterns', [])
                for pattern in patterns:
                    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.DOTALL)
                # Remove Hadoop-specific metadata if specified
                if settings.get('remove_metadata', True):
                    for pattern in self.metadata_patterns.values():
                        cleaned_text = re.sub(pattern, '', cleaned_text)
                # Preserve structured data if specified
                if settings.get('preserve_structured_data', False):
                    try:
                        if cleaned_text.strip().startswith('{') or cleaned_text.strip().startswith('['):
                            data = json.loads(cleaned_text)
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
        df[column] = df[column].apply(clean_text)
        return df
    
    def _extract_metadata(self, df: pd.DataFrame, column: str, settings: Dict[str, Any], original_df: pd.DataFrame) -> pd.DataFrame:
        """Extract Hadoop metadata into separate columns"""
        try:
            metadata_fields = settings.get('metadata_fields', list(self.metadata_patterns.keys()))
            for field in metadata_fields:
                if field in self.metadata_patterns:
                    pattern = self.metadata_patterns[field]
                    new_column = f"{column}_{field}"
                    df[new_column] = original_df[column].str.extract(pattern, expand=False)
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