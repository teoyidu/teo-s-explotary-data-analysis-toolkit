"""
Processor for detecting duplicate and near-duplicate content using MinHash/SimHash
"""

import logging
from typing import List, Dict, Any, Set, Tuple
import pandas as pd
import numpy as np
from datasketch import MinHash, MinHashLSH
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import sent_tokenize
import nltk

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

logger = logging.getLogger(__name__)

class DuplicateDetector:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the duplicate detector
        
        Args:
            config (Dict[str, Any]): Configuration dictionary with the following keys:
                - text_column: Column containing text to check for duplicates
                - similarity_threshold: Threshold for considering texts as duplicates (default: 0.9)
                - num_perm: Number of permutations for MinHash (default: 128)
                - shingle_size: Size of shingles for text tokenization (default: 3)
                - action: What to do with duplicates ('mark', 'remove', 'group')
        """
        self.config = config
        self.text_column = config.get('text_column')
        self.similarity_threshold = config.get('similarity_threshold', 0.9)
        self.num_perm = config.get('num_perm', 128)
        self.shingle_size = config.get('shingle_size', 3)
        self.action = config.get('action', 'mark')
        
        if not self.text_column:
            raise ValueError("text_column must be specified in config")
            
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
        Create MinHash objects for each text
        
        Args:
            texts (pd.Series): Series of texts to process
            
        Returns:
            List[MinHash]: List of MinHash objects
        """
        minhashes = []
        
        for text in texts:
            # Tokenize into sentences
            sentences = sent_tokenize(str(text))
            
            # Create shingles
            shingles = set()
            for sentence in sentences:
                words = sentence.split()
                for i in range(len(words) - self.shingle_size + 1):
                    shingle = ' '.join(words[i:i + self.shingle_size])
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
        df['is_duplicate'] = False
        df['duplicate_group'] = -1
        
        for group_idx, group in enumerate(duplicate_groups):
            for idx in group:
                df.loc[idx, 'is_duplicate'] = True
                df.loc[idx, 'duplicate_group'] = group_idx
                
        duplicate_count = df['is_duplicate'].sum()
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate texts in {len(duplicate_groups)} groups")
            
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
        logger.info(f"Removed {len(indices_to_remove)} duplicate texts")
        
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
        df['duplicate_group'] = -1
        
        for group_idx, group in enumerate(duplicate_groups):
            for idx in group:
                df.loc[idx, 'duplicate_group'] = group_idx
                
        logger.info(f"Grouped {len(duplicate_groups)} sets of duplicate texts")
        
        return df 