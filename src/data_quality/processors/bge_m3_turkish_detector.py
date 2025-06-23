"""
BGE-M3-based Turkish duplicate detector with multilingual semantic deduplication capabilities
"""

import logging
from typing import List, Dict, Any, Set, Tuple, Optional, Union
import pandas as pd
import numpy as np
import re
import unicodedata
from dataclasses import dataclass
import os
import time

# BGE-M3 imports
try:
    from FlagEmbedding import BGEM3FlagModel
    BGE_M3_AVAILABLE = True
except ImportError:
    BGE_M3_AVAILABLE = False
    print("Warning: FlagEmbedding not available. Install with: pip install FlagEmbedding")

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

class BGEM3TurkishDetector:
    """
    Turkish-friendly duplicate detector using BGE-M3 for multilingual semantic deduplication
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the BGE-M3-based Turkish duplicate detector
        
        Args:
            config (Dict[str, Any]): Configuration dictionary with the following keys:
                - text_column: Column containing text to check for duplicates
                - similarity_threshold: Threshold for considering texts as duplicates (default: 0.85)
                - action: What to do with duplicates ('mark', 'remove', 'group', 'bge_m3')
                - remove_stopwords: Whether to remove Turkish stop words (default: False)
                - normalize_text: Whether to normalize Turkish text (default: False)
                - min_word_length: Minimum word length to consider (default: 2)
                - use_fp16: Whether to use FP16 for faster computation (default: True)
                - max_length: Maximum sequence length (default: 512)
                - batch_size: Batch size for processing (default: 32)
                - retrieval_mode: Which retrieval mode to use ('dense', 'sparse', 'colbert', 'hybrid')
                - hybrid_weights: Weights for hybrid mode [dense_weight, sparse_weight, colbert_weight]
        """
        if not BGE_M3_AVAILABLE:
            raise ImportError("FlagEmbedding is required. Install with: pip install FlagEmbedding")
            
        self.config = config
        self.text_column = config.get('text_column')
        self.similarity_threshold = config.get('similarity_threshold', 0.85)
        self.action = config.get('action', 'bge_m3')
        self.remove_stopwords = config.get('remove_stopwords', False)
        self.normalize_text = config.get('normalize_text', False)
        self.min_word_length = config.get('min_word_length', 2)
        self.use_fp16 = config.get('use_fp16', True)
        self.max_length = config.get('max_length', 512)
        self.batch_size = config.get('batch_size', 32)
        self.retrieval_mode = config.get('retrieval_mode', 'dense')
        self.hybrid_weights = config.get('hybrid_weights', [0.4, 0.2, 0.4])
        
        if not self.text_column:
            raise ValueError("text_column must be specified in config")
            
        # Initialize Turkish stop words
        self.turkish_stopwords = self._load_turkish_stopwords()
        
        # Initialize BGE-M3 model
        self.bge_model: Any = self._initialize_bge_model()
        
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
        
    def _initialize_bge_model(self) -> Any:
        """
        Initialize BGE-M3 model for Turkish text processing
        
        Returns:
            BGEM3FlagModel instance
        """
        try:
            logger.info("Initializing BGE-M3 model...")
            model = BGEM3FlagModel('BAAI/bge-m3', use_fp16=self.use_fp16)
            logger.info("BGE-M3 model initialized successfully")
            return model
        except Exception as e:
            logger.error(f"Failed to initialize BGE-M3 model: {e}")
            raise
            
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
        Preprocess Turkish text for BGE-M3 processing
        
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
        
    def _compute_similarity_matrix(self, texts: List[str]) -> np.ndarray:
        """
        Compute similarity matrix using BGE-M3
        
        Args:
            texts (List[str]): List of texts to compare
            
        Returns:
            np.ndarray: Similarity matrix
        """
        try:
            if self.retrieval_mode == 'dense':
                # Use dense embeddings
                embeddings = self.bge_model.encode(texts, max_length=self.max_length)['dense_vecs']
                # Convert to numpy array if it's not already
                if not isinstance(embeddings, np.ndarray):
                    embeddings = np.array(embeddings)
                similarity_matrix = embeddings @ embeddings.T
                
            elif self.retrieval_mode == 'sparse':
                # Use sparse embeddings
                outputs = self.bge_model.encode(texts, return_dense=False, return_sparse=True, return_colbert_vecs=False)
                similarity_matrix = np.zeros((len(texts), len(texts)))
                
                for i in range(len(texts)):
                    for j in range(len(texts)):
                        if i == j:
                            similarity_matrix[i][j] = 1.0
                        else:
                            # Ensure lexical_weights are properly typed
                            lexical_weights_i = outputs['lexical_weights'][i]
                            lexical_weights_j = outputs['lexical_weights'][j]
                            
                            # Convert to proper format if needed
                            if isinstance(lexical_weights_i, np.ndarray):
                                lexical_weights_i = lexical_weights_i.tolist()
                            if isinstance(lexical_weights_j, np.ndarray):
                                lexical_weights_j = lexical_weights_j.tolist()
                                
                            score = self.bge_model.compute_lexical_matching_score(
                                lexical_weights_i, 
                                lexical_weights_j
                            )
                            similarity_matrix[i][j] = score
                            
            elif self.retrieval_mode == 'colbert':
                # Use ColBERT embeddings
                outputs = self.bge_model.encode(texts, return_dense=False, return_sparse=False, return_colbert_vecs=True)
                similarity_matrix = np.zeros((len(texts), len(texts)))
                
                for i in range(len(texts)):
                    for j in range(len(texts)):
                        if i == j:
                            similarity_matrix[i][j] = 1.0
                        else:
                            score = self.bge_model.colbert_score(
                                outputs['colbert_vecs'][i], 
                                outputs['colbert_vecs'][j]
                            )
                            similarity_matrix[i][j] = score
                            
            elif self.retrieval_mode == 'hybrid':
                # Use hybrid approach - convert list of lists to list of tuples
                sentence_pairs = [(texts[i], texts[j]) for i in range(len(texts)) for j in range(len(texts))]
                scores = self.bge_model.compute_score(
                    sentence_pairs, 
                    max_passage_length=self.max_length,
                    weights_for_different_modes=self.hybrid_weights
                )
                
                # Extract the combined score
                combined_scores = scores.get('colbert+sparse+dense', [])
                similarity_matrix = np.array(combined_scores).reshape(len(texts), len(texts))
                
            else:
                raise ValueError(f"Unknown retrieval mode: {self.retrieval_mode}")
                
            return similarity_matrix
            
        except Exception as e:
            logger.error(f"Error computing similarity matrix: {e}")
            raise
            
    def _find_duplicates(self, similarity_matrix: np.ndarray) -> List[List[int]]:
        """
        Find duplicate groups based on similarity matrix
        
        Args:
            similarity_matrix (np.ndarray): Similarity matrix
            
        Returns:
            List[List[int]]: List of duplicate groups
        """
        n = len(similarity_matrix)
        visited = [False] * n
        duplicate_groups = []
        
        for i in range(n):
            if visited[i]:
                continue
                
            # Find all texts similar to text i
            group = [i]
            visited[i] = True
            
            for j in range(i + 1, n):
                if not visited[j] and similarity_matrix[i][j] >= self.similarity_threshold:
                    group.append(j)
                    visited[j] = True
                    
            # Only add groups with more than one element
            if len(group) > 1:
                duplicate_groups.append(group)
                
        return duplicate_groups
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to detect duplicates using BGE-M3
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame with duplicate information
        """
        if self.text_column not in df.columns:
            logger.error(f"Column {self.text_column} not found in DataFrame")
            return df
            
        if self.action == 'bge_m3':
            return self._process_with_bge_m3(df)
        else:
            # Fallback to traditional processing for other actions
            return self._process_traditional(df)
            
    def _process_with_bge_m3(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process DataFrame using BGE-M3 for semantic deduplication
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        # Create a copy to avoid modifying the original
        df = df.copy()
        
        # Preprocess texts
        texts = df[self.text_column].apply(self._preprocess_turkish_text).tolist()
        
        logger.info(f"Computing similarity matrix for {len(texts)} texts using {self.retrieval_mode} mode...")
        start_time = time.time()
        
        # Compute similarity matrix
        similarity_matrix = self._compute_similarity_matrix(texts)
        
        compute_time = time.time() - start_time
        logger.info(f"Similarity matrix computed in {compute_time:.2f} seconds")
        
        # Find duplicate groups
        duplicate_groups = self._find_duplicates(similarity_matrix)
        
        # Initialize duplicate columns
        df['is_duplicate'] = False
        df['duplicate_group'] = -1
        
        # Mark duplicates
        for group_id, group in enumerate(duplicate_groups):
            # Keep the first item, mark others as duplicates
            for i, idx in enumerate(group):
                if i == 0:  # First item in group (representative)
                    df.iloc[idx]['is_duplicate'] = False
                    df.iloc[idx]['duplicate_group'] = group_id
                else:  # Duplicates
                    df.iloc[idx]['is_duplicate'] = True
                    df.iloc[idx]['duplicate_group'] = group_id
                    
        duplicate_count = df['is_duplicate'].sum()
        logger.info(f"BGE-M3 marked {duplicate_count} texts as duplicates out of {len(df)} total")
        logger.info(f"Found {len(duplicate_groups)} duplicate groups")
        
        return df
        
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
        
        # Count duplicate groups
        if 'duplicate_group' in result_df.columns:
            duplicate_groups = result_df[result_df['duplicate_group'] >= 0]['duplicate_group'].nunique()
        else:
            duplicate_groups = 0
        
        statistics = {
            'original_count': original_count,
            'duplicate_count': duplicate_count,
            'unique_count': unique_count,
            'duplicate_percentage': (duplicate_count / original_count * 100) if original_count > 0 else 0,
            'duplicate_groups': duplicate_groups,
            'method_used': 'bge_m3' if self.action == 'bge_m3' else 'traditional',
            'retrieval_mode': self.retrieval_mode,
            'similarity_threshold': self.similarity_threshold,
            'model_used': 'BGE-M3'
        }
        
        return result_df, statistics

# Backward compatibility class
class BGEM3DuplicateDetector(BGEM3TurkishDetector):
    """
    Backward compatibility class for BGE-M3-based duplicate detection
    """
    pass 