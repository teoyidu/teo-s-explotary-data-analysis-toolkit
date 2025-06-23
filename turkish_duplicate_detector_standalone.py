#!/usr/bin/env python3
"""
Standalone Turkish-friendly duplicate detector using MinHash/SimHash
"""

import logging
from typing import List, Dict, Any, Set, Tuple
import pandas as pd
import numpy as np
from datasketch import MinHash, MinHashLSH
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import nltk
import re
import unicodedata

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

# Try to download Turkish-specific data
try:
    nltk.data.find('tokenizers/punkt_tab/turkish')
except LookupError:
    try:
        nltk.download('punkt_tab')
    except:
        print("Warning: Turkish-specific tokenizer not available, using default tokenizer")

logger = logging.getLogger(__name__)

class TurkishDuplicateDetector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Turkish-friendly duplicate detector
        
        Args:
            config (Dict[str, Any]): Configuration dictionary with the following keys:
                - text_column: Column containing text to check for duplicates
                - similarity_threshold: Threshold for considering texts as duplicates (default: 0.9)
                - num_perm: Number of permutations for MinHash (default: 128)
                - shingle_size: Size of shingles for text tokenization (default: 3)
                - action: What to do with duplicates ('mark', 'remove', 'group')
                - remove_stopwords: Whether to remove Turkish stop words (default: True)
                - normalize_text: Whether to normalize Turkish text (default: True)
                - min_word_length: Minimum word length to consider (default: 2)
        """
        self.config = config
        self.text_column = config.get('text_column')
        self.similarity_threshold = config.get('similarity_threshold', 0.9)
        self.num_perm = config.get('num_perm', 128)
        self.shingle_size = config.get('shingle_size', 3)
        self.action = config.get('action', 'mark')
        self.remove_stopwords = config.get('remove_stopwords', True)
        self.normalize_text = config.get('normalize_text', True)
        self.min_word_length = config.get('min_word_length', 2)
        
        if not self.text_column:
            raise ValueError("text_column must be specified in config")
            
        # Initialize Turkish stop words
        self.turkish_stopwords = self._load_turkish_stopwords()
        
    def _load_turkish_stopwords(self) -> Set[str]:
        """
        Load Turkish stop words
        
        Returns:
            Set[str]: Set of Turkish stop words
        """
        try:
            # Try to get Turkish stop words from NLTK
            turkish_stops = set(stopwords.words('turkish'))
        except LookupError:
            # Fallback to a basic Turkish stop words list
            turkish_stops = {
                'acaba', 'ama', 'aslında', 'az', 'bazı', 'belki', 'biri', 'birkaç', 'birşey', 'biz', 'bu', 'çok', 'çünkü', 'da', 'daha', 'de', 'defa', 'diye', 'eğer', 'en', 'gibi', 'hem', 'hep', 'hepsi', 'her', 'hiç', 'için', 'ile', 'ise', 'kez', 'ki', 'kim', 'mı', 'mu', 'mü', 'nasıl', 'ne', 'neden', 'nerde', 'nerede', 'nereye', 'niçin', 'niye', 'o', 'sanki', 'şey', 'siz', 'şu', 'tüm', 've', 'veya', 'ya', 'yani'
            }
            
        return turkish_stops
        
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
        
    def _tokenize_turkish_text(self, text: str) -> List[str]:
        """
        Tokenize Turkish text into words
        
        Args:
            text (str): Input text
            
        Returns:
            List[str]: List of tokens
        """
        # Normalize text first
        text = self._normalize_turkish_text(text)
        
        # Tokenize into sentences
        try:
            sentences = sent_tokenize(text, language='turkish')
        except LookupError:
            # Fallback to default tokenization if Turkish-specific data is not available
            sentences = sent_tokenize(text)
        
        tokens = []
        for sentence in sentences:
            # Tokenize words
            try:
                words = word_tokenize(sentence, language='turkish')
            except LookupError:
                # Fallback to default word tokenization
                words = word_tokenize(sentence)
            
            for word in words:
                # Remove punctuation and numbers
                word = re.sub(r'[^\w\s]', '', word)
                word = re.sub(r'\d+', '', word)
                
                # Filter by length and stop words
                if (len(word) >= self.min_word_length and 
                    word.isalpha() and 
                    (not self.remove_stopwords or word not in self.turkish_stopwords)):
                    tokens.append(word)
                    
        return tokens
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to detect duplicates
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame with duplicate information
        """
        if self.text_column not in df.columns:
            logger.error(f"Column {self.text_column} not found in DataFrame")
            return df
            
        # Create shingles and MinHash for each text
        minhashes = self._create_minhashes(df[self.text_column])
        
        # Find duplicate groups
        duplicate_groups = self._find_duplicate_groups(minhashes)
        
        # Apply the specified action
        if self.action == 'mark':
            df = self._mark_duplicates(df, duplicate_groups)
        elif self.action == 'remove':
            df = self._remove_duplicates(df, duplicate_groups)
        elif self.action == 'group':
            df = self._group_duplicates(df, duplicate_groups)
            
        return df
        
    def _create_minhashes(self, texts: pd.Series) -> List[MinHash]:
        """
        Create MinHash objects for each text with Turkish-friendly processing
        
        Args:
            texts (pd.Series): Series of texts to process
            
        Returns:
            List[MinHash]: List of MinHash objects
        """
        minhashes = []
        
        for text in texts:
            # Tokenize Turkish text
            tokens = self._tokenize_turkish_text(str(text))
            
            # Create shingles from tokens
            shingles = set()
            for i in range(len(tokens) - self.shingle_size + 1):
                shingle = ' '.join(tokens[i:i + self.shingle_size])
                shingles.add(shingle)
                
            # Create MinHash
            minhash = MinHash(num_perm=self.num_perm)
            for shingle in shingles:
                minhash.update(shingle.encode('utf-8'))
                
            minhashes.append(minhash)
            
        return minhashes
        
    def _find_duplicate_groups(self, minhashes: List[MinHash]) -> List[Set[int]]:
        """
        Find groups of duplicate texts using LSH
        
        Args:
            minhashes (List[MinHash]): List of MinHash objects
            
        Returns:
            List[Set[int]]: List of sets containing indices of duplicate texts
        """
        # Create LSH index
        lsh = MinHashLSH(threshold=self.similarity_threshold, num_perm=self.num_perm)
        
        # Add all MinHashes to LSH
        for i, minhash in enumerate(minhashes):
            lsh.insert(i, minhash)
            
        # Find duplicate groups
        duplicate_groups = []
        processed = set()
        
        for i, minhash in enumerate(minhashes):
            if i in processed:
                continue
                
            # Find similar items
            similar_items = lsh.query(minhash)
            if len(similar_items) > 1:
                duplicate_groups.append(set(similar_items))
                processed.update(similar_items)
                
        return duplicate_groups
        
    def _mark_duplicates(self, df: pd.DataFrame, duplicate_groups: List[Set[int]]) -> pd.DataFrame:
        """
        Mark duplicate rows with a flag
        
        Args:
            df (pd.DataFrame): Input DataFrame
            duplicate_groups (List[Set[int]]): List of sets containing indices of duplicate texts
            
        Returns:
            pd.DataFrame: Processed DataFrame with duplicate flag
        """
        df = df.copy()
        df['is_duplicate'] = False
        df['duplicate_group'] = -1
        
        for group_idx, group in enumerate(duplicate_groups):
            for idx in group:
                df.loc[idx, 'is_duplicate'] = True
                df.loc[idx, 'duplicate_group'] = group_idx
                
        duplicate_count = df['is_duplicate'].sum()
        if duplicate_count > 0:
            print(f"Found {duplicate_count} duplicate texts in {len(duplicate_groups)} groups")
            
        return df
        
    def _remove_duplicates(self, df: pd.DataFrame, duplicate_groups: List[Set[int]]) -> pd.DataFrame:
        """
        Remove duplicate rows, keeping the first occurrence
        
        Args:
            df (pd.DataFrame): Input DataFrame
            duplicate_groups (List[Set[int]]): List of sets containing indices of duplicate texts
            
        Returns:
            pd.DataFrame: Processed DataFrame with duplicates removed
        """
        indices_to_remove = set()
        
        for group in duplicate_groups:
            # Keep the first item in each group
            first_idx = min(group)
            indices_to_remove.update(group - {first_idx})
            
        df = df.drop(index=list(indices_to_remove))
        print(f"Removed {len(indices_to_remove)} duplicate texts")
        
        return df
        
    def _group_duplicates(self, df: pd.DataFrame, duplicate_groups: List[Set[int]]) -> pd.DataFrame:
        """
        Group duplicate rows together
        
        Args:
            df (pd.DataFrame): Input DataFrame
            duplicate_groups (List[Set[int]]): List of sets containing indices of duplicate texts
            
        Returns:
            pd.DataFrame: Processed DataFrame with duplicate groups
        """
        df = df.copy()
        df['duplicate_group'] = -1
        
        for group_idx, group in enumerate(duplicate_groups):
            for idx in group:
                df.loc[idx, 'duplicate_group'] = group_idx
                
        print(f"Grouped {len(duplicate_groups)} sets of duplicate texts")
        
        return df

def main():
    """Example usage of Turkish duplicate detector"""
    
    # Sample Turkish text data
    sample_data = {
        'text': [
            "Merhaba, nasılsınız? Bugün hava çok güzel.",
            "Merhaba, nasılsınız? Bugün hava çok güzel.",  # Exact duplicate
            "Merhaba! Nasılsınız? Bugün hava çok güzel.",  # Near duplicate with punctuation
            "Merhaba, nasılsınız? Bugün hava çok güzel.",  # Another exact duplicate
            "Bugün hava çok güzel ve güneşli.",  # Different text
            "Merhaba, nasılsınız? Bugün hava çok güzel.",  # Another exact duplicate
            "Bugün hava çok güzel ve güneşli.",  # Duplicate of the different text
            "İstanbul'da yaşıyorum ve çok mutluyum.",  # Unique text
            "İstanbul'da yaşıyorum ve çok mutluyum.",  # Duplicate of unique text
        ]
    }
    
    df = pd.DataFrame(sample_data)
    
    print("Original DataFrame:")
    print(df)
    print("\n" + "="*50 + "\n")
    
    # Configuration for Turkish duplicate detection
    config = {
        'text_column': 'text',
        'similarity_threshold': 0.8,  # Lower threshold for Turkish text
        'num_perm': 128,
        'shingle_size': 3,
        'action': 'mark',  # Mark duplicates
        'remove_stopwords': True,
        'normalize_text': True,
        'min_word_length': 2
    }
    
    # Initialize Turkish duplicate detector
    detector = TurkishDuplicateDetector(config)
    
    # Process the DataFrame
    result_df = detector.process(df)
    
    print("Processed DataFrame with duplicate detection:")
    print(result_df)
    print("\n" + "="*50 + "\n")
    
    # Show duplicate groups
    duplicate_groups = result_df[result_df['duplicate_group'] >= 0].groupby('duplicate_group')
    
    print("Duplicate Groups Found:")
    for group_id, group in duplicate_groups:
        print(f"\nGroup {group_id}:")
        for idx, row in group.iterrows():
            print(f"  Row {idx}: {row['text']}")
    
    print("\n" + "="*50 + "\n")
    
    # Example with removal action
    print("Example with removal action:")
    config_remove = config.copy()
    config_remove['action'] = 'remove'
    
    detector_remove = TurkishDuplicateDetector(config_remove)
    result_remove = detector_remove.process(df)
    
    print(f"Original rows: {len(df)}")
    print(f"After removing duplicates: {len(result_remove)}")
    print(f"Removed {len(df) - len(result_remove)} duplicate rows")
    
    print("\nRemaining unique texts:")
    for idx, row in result_remove.iterrows():
        print(f"  {row['text']}")

if __name__ == "__main__":
    main() 