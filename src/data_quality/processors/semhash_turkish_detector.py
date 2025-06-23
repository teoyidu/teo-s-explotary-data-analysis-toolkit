"""
SemHash-based Turkish duplicate detector with semantic deduplication capabilities
"""

import logging
from typing import List, Dict, Any, Set, Tuple, Optional
import pandas as pd
import numpy as np
import re
import unicodedata
from dataclasses import dataclass
import os

# SemHash imports
try:
    from semhash import SemHash
    SEMHASH_AVAILABLE = True
except ImportError:
    SEMHASH_AVAILABLE = False
    print("Warning: SemHash not available. Install with: pip install semhash")

# NLTK imports for Turkish text processing
try:
    from nltk.tokenize import sent_tokenize, word_tokenize
    from nltk.corpus import stopwords
    import nltk
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False
    print("Warning: NLTK not available. Install with: pip install nltk")

logger = logging.getLogger(__name__)

# Download required NLTK data
if NLTK_AVAILABLE:
    try:
        nltk.data.find('tokenizers/punkt')
    except LookupError:
        nltk.download('punkt')

    try:
        nltk.data.find('corpora/stopwords')
    except LookupError:
        nltk.download('stopwords')

def _load_stopwords_from_file(filepath: str) -> Optional[Set[str]]:
    """Load stopwords from a text file, one word per line."""
    if not os.path.exists(filepath):
        return None
    stopwords_set = set()
    with open(filepath, encoding='utf-8') as f:
        for line in f:
            word = line.strip()
            if word:
                stopwords_set.add(word)
    return stopwords_set if stopwords_set else None

@dataclass
class DeduplicationResult:
    """Result of deduplication process"""
    selected_records: List[Dict[str, Any]]
    removed_records: List[Dict[str, Any]]
    duplicate_groups: List[List[int]]
    statistics: Dict[str, Any]

class SemHashTurkishDetector:
    """
    Turkish-friendly duplicate detector using SemHash for semantic deduplication
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the SemHash-based Turkish duplicate detector
        
        Args:
            config (Dict[str, Any]): Configuration dictionary with the following keys:
                - text_column: Column containing text to check for duplicates
                - similarity_threshold: Threshold for considering texts as duplicates (default: 0.8)
                - action: What to do with duplicates ('mark', 'remove', 'group', 'semhash')
                - remove_stopwords: Whether to remove Turkish stop words (default: True)
                - normalize_text: Whether to normalize Turkish text (default: True)
                - min_word_length: Minimum word length to consider (default: 2)
                - use_multilingual_model: Whether to use multilingual model (default: True)
                - model_name: Custom model name for SemHash (optional)
                - use_ann: Whether to use ANN backend (default: True)
                - batch_size: Batch size for processing (default: 1000)
        """
        if not SEMHASH_AVAILABLE:
            raise ImportError("SemHash is required. Install with: pip install semhash")
            
        self.config = config
        self.text_column = config.get('text_column')
        self.similarity_threshold = config.get('similarity_threshold', 0.85)
        self.action = config.get('action', 'semhash')
        self.remove_stopwords = config.get('remove_stopwords', False)
        self.normalize_text = config.get('normalize_text', True)
        self.min_word_length = config.get('min_word_length', 2)
        self.use_multilingual_model = config.get('use_multilingual_model', True)
        self.model_name = config.get('model_name')
        self.use_ann = config.get('use_ann', True)
        self.batch_size = config.get('batch_size', 1000)
        
        if not self.text_column:
            raise ValueError("text_column must be specified in config")
            
        # Initialize Turkish stop words
        self.turkish_stopwords = self._load_turkish_stopwords()
        
        # Initialize SemHash model
        self.semhash_model = self._initialize_semhash_model()
        
    def _load_turkish_stopwords(self) -> Set[str]:
        """
        Load Turkish stop words from turkish 3.txt if available, else NLTK, else fallback.
        """
        # 1. Try to load from turkish 3.txt in the project root
        stopwords_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'turkish 3.txt')
        stopwords_set = _load_stopwords_from_file(stopwords_path)
        if stopwords_set:
            logger.info(f"Loaded Turkish stopwords from {stopwords_path} ({len(stopwords_set)} words)")
            return stopwords_set
        # 2. Try NLTK
        if NLTK_AVAILABLE:
            try:
                turkish_stops = set(stopwords.words('turkish'))
                logger.info(f"Loaded Turkish stopwords from NLTK ({len(turkish_stops)} words)")
                return turkish_stops
            except LookupError:
                pass
        # 3. Fallback
        fallback = {
            'acaba', 'ama', 'aslında', 'az', 'bazı', 'belki', 'biri', 'birkaç', 'birşey', 'biz', 'bu', 'çok', 'çünkü', 'da', 'daha', 'de', 'defa', 'diye', 'eğer', 'en', 'gibi', 'hem', 'hep', 'hepsi', 'her', 'hiç', 'için', 'ile', 'ise', 'kez', 'ki', 'kim', 'mı', 'mu', 'mü', 'nasıl', 'ne', 'neden', 'nerde', 'nerede', 'nereye', 'niçin', 'niye', 'o', 'sanki', 'şey', 'siz', 'şu', 'tüm', 've', 'veya', 'ya', 'yani'
        }
        logger.warning("Falling back to built-in Turkish stopwords list.")
        return fallback
        
    def _initialize_semhash_model(self):
        """
        Initialize SemHash model for Turkish text processing
        
        Returns:
            Model instance or None for default
        """
        # SemHash handles model initialization internally
        # We'll pass the model name as a parameter to from_records
        return None
            
    def _normalize_turkish_text(self, text: str) -> str:
        """
        Normalize Turkish text by handling special characters and diacritics
        
        Args:
            text (str): Input text
            
        Returns:
            str: Normalized text
        """
        if not self.normalize_text:
            return text
            
        # Convert to lowercase
        text = text.lower()
        
        # Normalize unicode characters
        text = unicodedata.normalize('NFKC', text)
        
        # Handle Turkish specific characters
        text = text.replace('ı', 'i')
        text = text.replace('ğ', 'g')
        text = text.replace('ü', 'u')
        text = text.replace('ş', 's')
        text = text.replace('ö', 'o')
        text = text.replace('ç', 'c')
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text
        
    def _preprocess_turkish_text(self, text: str) -> str:
        """
        Preprocess Turkish text for SemHash processing
        
        Args:
            text (str): Input text
            
        Returns:
            str: Preprocessed text
        """
        # Normalize text
        text = self._normalize_turkish_text(str(text))
        
        if self.remove_stopwords and NLTK_AVAILABLE:
            # Tokenize and remove stop words
            try:
                sentences = sent_tokenize(text)
                processed_sentences = []
                
                for sentence in sentences:
                    try:
                        words = word_tokenize(sentence)
                        filtered_words = []
                        
                        for word in words:
                            # Remove punctuation and numbers
                            word = re.sub(r'[^\w\s]', '', word)
                            word = re.sub(r'\d+', '', word)
                            
                            # Filter by length and stop words
                            if (len(word) >= self.min_word_length and 
                                word.isalpha() and 
                                word not in self.turkish_stopwords):
                                filtered_words.append(word)
                                
                        if filtered_words:
                            processed_sentences.append(' '.join(filtered_words))
                    except:
                        # Fallback to simple processing
                        processed_sentences.append(sentence)
                        
                text = ' '.join(processed_sentences)
            except:
                # Fallback to original text if tokenization fails
                pass
                
        return text
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to detect duplicates using SemHash
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame with duplicate information
        """
        if self.text_column not in df.columns:
            logger.error(f"Column {self.text_column} not found in DataFrame")
            return df
            
        if self.action == 'semhash':
            return self._process_with_semhash(df)
        else:
            # Fallback to traditional processing for other actions
            return self._process_traditional(df)
            
    def _process_with_semhash(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process DataFrame using SemHash for semantic deduplication
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Preprocess texts
        texts = df[self.text_column].apply(self._preprocess_turkish_text).tolist()
        
        # Convert DataFrame to records format for SemHash
        records = df.to_dict(orient='records')
        
        try:
            # Ensure text_column is a string
            if not isinstance(self.text_column, str):
                raise ValueError(f"text_column must be a string, got {type(self.text_column)}")
                
            # Initialize SemHash with records - use default model
            semhash = SemHash.from_records(
                records=records,
                columns=[self.text_column],
                use_ann=self.use_ann
            )
            
            # Perform self-deduplication
            result = semhash.self_deduplicate()
            
            # Initialize duplicate columns
            df['is_duplicate'] = False
            df['duplicate_group'] = -1
            
            # Create a mapping from original indices to result indices
            # SemHash returns selected records, we need to find which original records were kept
            selected_records = result.selected
            selected_texts = [record[self.text_column] for record in selected_records]
            
            # Mark duplicates by comparing with selected texts
            for idx, row in df.iterrows():
                original_text = row[self.text_column]
                preprocessed_text = self._preprocess_turkish_text(original_text)
                
                # Check if this text is in the selected records
                if preprocessed_text not in selected_texts:
                    df.at[idx, 'is_duplicate'] = True
                    df.at[idx, 'duplicate_group'] = 0  # All removed items in group 0
            
            duplicate_count = df['is_duplicate'].sum()
            logger.info(f"SemHash marked {duplicate_count} texts as duplicates out of {len(df)} total")
            
            return df
            
        except Exception as e:
            logger.error(f"SemHash processing failed: {e}. Falling back to traditional method.")
            return self._process_traditional(df)
            
    def _process_traditional(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process DataFrame using traditional duplicate detection methods
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        # This is a simplified version - in practice, you might want to use
        # the existing TurkishDuplicateDetector here
        df = df.copy()
        df['is_duplicate'] = False
        df['duplicate_group'] = -1
        
        # Simple exact duplicate detection
        duplicates = df.duplicated(subset=[self.text_column], keep='first')
        df.loc[duplicates, 'is_duplicate'] = True
        df.loc[duplicates, 'duplicate_group'] = 0
        
        return df
        
    def process_with_statistics(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Process DataFrame and return detailed statistics
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            Tuple[pd.DataFrame, Dict[str, Any]]: Processed DataFrame and statistics
        """
        original_count = len(df)
        
        # Process the DataFrame
        result_df = self.process(df)
        
        # Calculate statistics
        duplicate_count = result_df['is_duplicate'].sum() if 'is_duplicate' in result_df.columns else 0
        unique_count = len(result_df) - duplicate_count
        
        statistics = {
            'original_count': original_count,
            'duplicate_count': duplicate_count,
            'unique_count': unique_count,
            'duplicate_percentage': (duplicate_count / original_count * 100) if original_count > 0 else 0,
            'method_used': 'semhash' if self.action == 'semhash' else 'traditional',
            'similarity_threshold': self.similarity_threshold,
            'model_used': self.model_name or ('multilingual' if self.use_multilingual_model else 'default')
        }
        
        return result_df, statistics

# Backward compatibility class
class SemHashDuplicateDetector(SemHashTurkishDetector):
    """
    Backward compatibility class for SemHash-based duplicate detection
    """
    pass 