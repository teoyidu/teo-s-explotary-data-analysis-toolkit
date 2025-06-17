"""
Processor for cleaning HTML tags from text data
"""

import logging
from typing import List, Dict, Any, Set
import pandas as pd
from bs4 import BeautifulSoup
import re

logger = logging.getLogger(__name__)

class HTMLCleanerProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.html_columns = config.get('html_columns', {})
        
        # Default whitelist of tags to preserve
        self.default_whitelist = {'p', 'br', 'strong', 'em', 'b', 'i', 'u', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'}
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to clean HTML tags
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.html_columns:
            return df
            
        for column, settings in self.html_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply HTML cleaning
            df = self._clean_html(df, column, settings)
            
        return df
        
    def _clean_html(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean HTML tags from text data"""
        def clean_text(text):
            try:
                # Parse HTML
                soup = BeautifulSoup(text, 'html.parser')
                
                # Get whitelist of tags to preserve
                whitelist = set(settings.get('whitelist_tags', self.default_whitelist))
                
                # Handle custom tag replacements
                custom_replacements = settings.get('custom_tag_replacements', {})
                for tag, replacement in custom_replacements.items():
                    for element in soup.find_all(tag):
                        element.replace_with(replacement)
                
                # Remove non-whitelisted tags but keep their content
                for tag in soup.find_all():
                    if tag.name not in whitelist:
                        tag.unwrap()
                
                # Handle specific attributes if needed
                if settings.get('preserve_attributes', False):
                    for tag in soup.find_all():
                        if tag.name in whitelist:
                            # Keep specified attributes
                            allowed_attrs = settings.get('allowed_attributes', {}).get(tag.name, [])
                            attrs_to_remove = [attr for attr in tag.attrs if attr not in allowed_attrs]
                            for attr in attrs_to_remove:
                                del tag[attr]
                
                # Get text content with preserved formatting
                cleaned_text = str(soup)
                
                # Handle specific HTML entities if needed
                if settings.get('handle_entities', True):
                    entity_mapping = settings.get('custom_entities', {
                        '&nbsp;': ' ',
                        '&amp;': '&',
                        '&lt;': '<',
                        '&gt;': '>',
                        '&quot;': '"',
                        '&apos;': "'"
                    })
                    for entity, replacement in entity_mapping.items():
                        cleaned_text = cleaned_text.replace(entity, replacement)
                
                # Remove extra whitespace
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                
                # Apply custom transformations if specified
                if 'custom_transformations' in settings:
                    for transform in settings['custom_transformations']:
                        if transform['type'] == 'replace':
                            cleaned_text = re.sub(transform['pattern'], transform['replacement'], cleaned_text)
                        elif transform['type'] == 'extract':
                            match = re.search(transform['pattern'], cleaned_text)
                            if match:
                                cleaned_text = match.group(1)
                
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning HTML in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df 