"""
Processor for cleaning boilerplate text from data
"""

import logging
from typing import List, Dict, Any, Set, Optional, Tuple
import pandas as pd
import re
from difflib import SequenceMatcher
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from collections import defaultdict

logger = logging.getLogger(__name__)

class BoilerplateCleanerProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.boilerplate_columns = config.get('boilerplate_columns', {})
        
        # Common boilerplate patterns
        self.common_patterns = [
            r'^\s*Copyright.*?$',  # Copyright notices
            r'^\s*All rights reserved.*?$',  # Rights reserved notices
            r'^\s*Confidential.*?$',  # Confidentiality notices
            r'^\s*Proprietary.*?$',  # Proprietary notices
            r'^\s*Page \d+ of \d+.*?$',  # Page numbers
            r'^\s*Generated on.*?$',  # Generation timestamps
            r'^\s*Last updated.*?$',  # Last updated timestamps
            r'^\s*Version \d+\.\d+.*?$',  # Version numbers
            r'^\s*Document ID:.*?$',  # Document IDs
            r'^\s*Reference:.*?$',  # Reference numbers
        ]
        
        # Initialize TF-IDF vectorizer for smart duplicate detection
        self.vectorizer = TfidfVectorizer(
            stop_words='english',
            ngram_range=(1, 2),
            max_features=1000
        )
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to clean boilerplate text
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.boilerplate_columns:
            return df
            
        for column, settings in self.boilerplate_columns.items():
            if column not in df.columns:
                continue
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply boilerplate cleaning
            df = self._clean_boilerplate(df, column, settings)
            
        return df
        
    def _clean_boilerplate(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean boilerplate text from data"""
        def clean_text(text):
            try:
                cleaned_text = text
                
                # Remove common boilerplate patterns
                for pattern in self.common_patterns:
                    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.MULTILINE)
                
                # Remove custom boilerplate patterns if specified
                if 'custom_patterns' in settings:
                    for pattern in settings['custom_patterns']:
                        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.MULTILINE)
                
                # Remove duplicate content if specified
                if settings.get('remove_duplicates', False):
                    cleaned_text = self._remove_duplicates(cleaned_text, settings)
                
                # Remove header/footer if specified
                if settings.get('remove_header_footer', False):
                    cleaned_text = self._remove_header_footer(cleaned_text, settings)
                
                # Apply template matching if specified
                if settings.get('template_matching', False):
                    cleaned_text = self._apply_template_matching(cleaned_text, settings)
                
                # Apply context-aware cleaning if specified
                if settings.get('context_aware_cleaning', False):
                    cleaned_text = self._apply_context_aware_cleaning(cleaned_text, settings)
                
                # Remove extra whitespace
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning boilerplate in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df
        
    def _remove_duplicates(self, text: str, settings: Dict) -> str:
        """Remove duplicate content using smart detection"""
        try:
            # Split into lines
            lines = text.split('\n')
            
            if settings.get('use_tfidf', False):
                # Use TF-IDF for more sophisticated duplicate detection
                if len(lines) > 1:
                    # Create TF-IDF matrix
                    tfidf_matrix = self.vectorizer.fit_transform(lines)
                    
                    # Calculate cosine similarity
                    similarity_matrix = cosine_similarity(tfidf_matrix)
                    
                    # Find duplicates
                    duplicates = set()
                    for i in range(len(lines)):
                        for j in range(i + 1, len(lines)):
                            if similarity_matrix[i, j] > settings.get('similarity_threshold', 0.8):
                                duplicates.add(j)
                    
                    # Remove duplicates
                    unique_lines = [line for i, line in enumerate(lines) if i not in duplicates]
                else:
                    unique_lines = lines
            else:
                # Use simpler similarity matching
                unique_lines = []
                for line in lines:
                    if not any(SequenceMatcher(None, line, existing).ratio() > 
                             settings.get('similarity_threshold', 0.9) 
                             for existing in unique_lines):
                        unique_lines.append(line)
            
            return '\n'.join(unique_lines)
        except Exception as e:
            logger.warning(f"Error in duplicate removal: {str(e)}")
            return text
            
    def _remove_header_footer(self, text: str, settings: Dict) -> str:
        """Remove headers and footers using smart detection"""
        try:
            lines = text.split('\n')
            if len(lines) <= 4:  # Not enough lines to process
                return text
                
            # Calculate line frequencies
            line_freq = defaultdict(int)
            for line in lines:
                line_freq[line.strip()] += 1
                
            # Find potential headers/footers (lines that appear multiple times)
            potential_boilerplate = {line for line, freq in line_freq.items() 
                                  if freq > 1 and len(line) > 0}
            
            # Remove boilerplate lines
            cleaned_lines = [line for line in lines 
                           if line.strip() not in potential_boilerplate]
            
            return '\n'.join(cleaned_lines)
        except Exception as e:
            logger.warning(f"Error in header/footer removal: {str(e)}")
            return text
            
    def _apply_template_matching(self, text: str, settings: Dict) -> str:
        """Apply template matching to identify and remove boilerplate"""
        try:
            if 'templates' not in settings:
                return text
                
            # Get template patterns
            templates = settings['templates']
            
            # Try to match each template
            for template in templates:
                pattern = template.get('pattern')
                if pattern:
                    # Replace template matches with specified replacement
                    replacement = template.get('replacement', '')
                    text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
            
            return text
        except Exception as e:
            logger.warning(f"Error in template matching: {str(e)}")
            return text
            
    def _apply_context_aware_cleaning(self, text: str, settings: Dict) -> str:
        """Apply context-aware cleaning based on surrounding text"""
        try:
            if 'context_rules' not in settings:
                return text
                
            # Get context rules
            context_rules = settings['context_rules']
            
            # Apply each rule
            for rule in context_rules:
                # Check if context matches
                if re.search(rule.get('context_pattern', ''), text):
                    # Apply the cleaning action
                    if rule.get('action') == 'remove':
                        text = re.sub(rule.get('pattern', ''), '', text)
                    elif rule.get('action') == 'replace':
                        text = re.sub(rule.get('pattern', ''), 
                                    rule.get('replacement', ''), text)
            
            return text
        except Exception as e:
            logger.warning(f"Error in context-aware cleaning: {str(e)}")
            return text 