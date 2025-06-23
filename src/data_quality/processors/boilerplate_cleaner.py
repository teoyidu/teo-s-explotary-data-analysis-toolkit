"""
Enhanced processor for cleaning boilerplate text from Turkish legal data
"""

import logging
from typing import List, Dict, Any, Set, Optional, Tuple, Union
import pandas as pd
import re
from difflib import SequenceMatcher
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from collections import defaultdict
import unicodedata
import os

# Turkish-specific imports
try:
    from nltk.tokenize import sent_tokenize, word_tokenize
    from nltk.corpus import stopwords
    import nltk
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False

# BGE-M3 imports for Turkish embeddings
try:
    from FlagEmbedding import BGEM3FlagModel
    BGE_M3_AVAILABLE = True
except ImportError:
    BGE_M3_AVAILABLE = False

# SemHash imports for fast semantic deduplication
try:
    from semhash import SemHash
    SEMHASH_AVAILABLE = True
except ImportError:
    SEMHASH_AVAILABLE = False

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

def _load_turkish_stopwords() -> Set[str]:
    """Load Turkish stop words from file or fallback to built-in list"""
    # Try to load from turkish 3.txt in the project root
    # Get the project root directory (3 levels up from this file)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
    stopwords_path = os.path.join(project_root, 'turkish 3.txt')
    
    logger.info(f"Looking for Turkish stopwords at: {stopwords_path}")
    
    if os.path.exists(stopwords_path):
        stopwords_set = set()
        with open(stopwords_path, encoding='utf-8') as f:
            for line in f:
                word = line.strip()
                if word:
                    stopwords_set.add(word)
        if stopwords_set:
            logger.info(f"Loaded Turkish stopwords from {stopwords_path} ({len(stopwords_set)} words)")
            return stopwords_set
        else:
            logger.warning(f"Turkish stopwords file exists but is empty: {stopwords_path}")
    else:
        logger.warning(f"Turkish stopwords file not found: {stopwords_path}")
    
    # Fallback to NLTK Turkish stopwords
    if NLTK_AVAILABLE:
        try:
            turkish_stops = set(stopwords.words('turkish'))
            logger.info(f"Loaded Turkish stopwords from NLTK ({len(turkish_stops)} words)")
            return turkish_stops
        except LookupError:
            logger.warning("NLTK Turkish stopwords not available")
            pass
    
    # Final fallback
    fallback = {
        'acaba', 'ama', 'aslında', 'az', 'bazı', 'belki', 'biri', 'birkaç', 'birşey', 'biz', 'bu', 'çok', 'çünkü', 
        'da', 'daha', 'de', 'defa', 'diye', 'eğer', 'en', 'gibi', 'hem', 'hep', 'hepsi', 'her', 'hiç', 'için', 
        'ile', 'ise', 'kez', 'ki', 'kim', 'mı', 'mu', 'mü', 'nasıl', 'ne', 'neden', 'nerde', 'nerede', 'nereye', 
        'niçin', 'niye', 'o', 'sanki', 'şey', 'siz', 'şu', 'tüm', 've', 'veya', 'ya', 'yani'
    }
    logger.warning("Using fallback Turkish stopwords list")
    return fallback

class TurkishBoilerplateCleanerProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the enhanced Turkish boilerplate cleaner
        
        Args:
            config (Dict[str, Any]): Configuration dictionary with the following keys:
                - boilerplate_columns: Dictionary mapping column names to settings
                - use_turkish_embeddings: Whether to use Turkish-specific embedding models (default: True)
                - embedding_model: Which embedding model to use ('bge_m3', 'semhash', 'tfidf') (default: 'bge_m3')
                - similarity_threshold: Threshold for duplicate detection (default: 0.8)
                - remove_turkish_stopwords: Whether to remove Turkish stop words (default: True)
                - normalize_turkish_text: Whether to normalize Turkish text (default: True)
                - use_legal_patterns: Whether to use Turkish legal boilerplate patterns (default: True)
        """
        self.config = config
        self.boilerplate_columns = config.get('boilerplate_columns', {})
        self.use_turkish_embeddings = config.get('use_turkish_embeddings', True)
        self.embedding_model = config.get('embedding_model', 'bge_m3')
        self.similarity_threshold = config.get('similarity_threshold', 0.8)
        self.remove_turkish_stopwords = config.get('remove_turkish_stopwords', True)
        self.normalize_turkish_text = config.get('normalize_turkish_text', True)
        self.use_legal_patterns = config.get('use_legal_patterns', True)
        
        # Debug mode - enable detailed logging
        self.debug_mode = config.get('debug_mode', False)
        
        # Load Turkish stop words
        self.turkish_stopwords = _load_turkish_stopwords()
        
        # Turkish legal boilerplate patterns - UPDATED to work without line boundaries
        self.turkish_legal_patterns = [
            # Document headers and footers - REMOVED line boundaries
            r'TÜRKİYE CUMHURİYETİ.*?',  # Republic of Turkey headers
            r'T\.C\.\s*.*?',  # T.C. (Turkish Republic) headers
            r'Sayfa\s+\d+\s+/\s+\d+.*?',  # Page numbers in Turkish
            r'Sayfa\s+\d+.*?',  # Page numbers
            r'Belge\s+No:\s*.*?',  # Document numbers
            r'Referans\s+No:\s*.*?',  # Reference numbers
            r'Dosya\s+No:\s*.*?',  # File numbers
            r'Kayıt\s+No:\s*.*?',  # Record numbers
            
            # Legal document boilerplate - REMOVED line boundaries
            r'Bu\s+sözleşme\s+taraflar\s+arasında\s+imzalanmıştır.*?',  # Contract boilerplate
            r'Taraflar\s+arasında\s+imzalanan\s+sözleşme.*?',  # Contract between parties
            r'Mahkeme\s+kararı\s+kesinleşmiştir.*?',  # Court decision finalized
            r'Yargıtay\s+kararı\s+kesinleşmiştir.*?',  # Supreme Court decision finalized
            r'Danıştay\s+kararı\s+kesinleşmiştir.*?',  # Council of State decision finalized
            r'Anayasa\s+Mahkemesi\s+kararı.*?',  # Constitutional Court decision
            r'Bu\s+karar\s+kesinleşmiştir.*?',  # This decision is final
            r'Karar\s+kesinleşmiştir.*?',  # Decision is final
            
            # Legal document metadata - REMOVED line boundaries
            r'Tarih:\s*\d{1,2}\.\d{1,2}\.\d{4}.*?',  # Date patterns
            r'Tarih\s+ve\s+saat:\s*.*?',  # Date and time
            r'İmza\s+tarihi:\s*.*?',  # Signature date
            r'Yürürlük\s+tarihi:\s*.*?',  # Effective date
            r'Yayın\s+tarihi:\s*.*?',  # Publication date
            
            # Legal document structure - REMOVED line boundaries
            r'Madde\s+\d+.*?',  # Article numbers
            r'Bölüm\s+\d+.*?',  # Section numbers
            r'Kısım\s+\d+.*?',  # Part numbers
            r'Fıkra\s+\d+.*?',  # Paragraph numbers
            r'Bent\s+\d+.*?',  # Subparagraph numbers
            
            # Legal document boilerplate phrases - REMOVED line boundaries
            r'Yukarıda\s+adı\s+ve\s+soyadı\s+yazılı.*?',  # Above named person
            r'Taraflar\s+arasında\s+anlaşma\s+sağlanmıştır.*?',  # Agreement reached between parties
            r'Bu\s+belge\s+ile\s+ilgili\s+.*?',  # Regarding this document
            r'Belgenin\s+devamı\s+.*?',  # Continuation of document
            r'Ek\s+\d+.*?',  # Appendix numbers
            r'Ekler\s+.*?',  # Appendices
            
            # Court and legal institution boilerplate - REMOVED line boundaries
            r'Mahkeme\s+adı:\s*.*?',  # Court name
            r'Mahkeme\s+merkezi:\s*.*?',  # Court location
            r'Duruşma\s+tarihi:\s*.*?',  # Hearing date
            r'Duruşma\s+saati:\s*.*?',  # Hearing time
            r'Duruşma\s+yeri:\s*.*?',  # Hearing location
            
            # Legal document classification - REMOVED line boundaries
            r'Dava\s+türü:\s*.*?',  # Case type
            r'Dava\s+konusu:\s*.*?',  # Subject of case
            r'Talep\s+edilen:\s*.*?',  # Requested
            r'Talep\s+eden:\s*.*?',  # Petitioner
            r'Davalı:\s*.*?',  # Defendant
            
            # Legal document status - REMOVED line boundaries
            r'Durum:\s*.*?',  # Status
            r'Sonuç:\s*.*?',  # Result
            r'Karar:\s*.*?',  # Decision
            r'Hüküm:\s*.*?',  # Judgment
            
            # Legal document formatting - REMOVED line boundaries
            r'GİRİŞ.*?',  # Introduction
            r'SONUÇ.*?',  # Conclusion
            r'KARAR.*?',  # Decision
            r'HÜKÜM.*?',  # Judgment
            r'GEREKÇE.*?',  # Reasoning
            r'MADDELER.*?',  # Articles
            r'EK.*?',  # Appendix
            
            # Legal document boilerplate endings - REMOVED line boundaries
            r'Bu\s+karar\s+ile\s+ilgili\s+.*?',  # Regarding this decision
            r'Karar\s+bu\s+şekilde\s+verilmiştir.*?',  # Decision given in this manner
            r'Hüküm\s+bu\s+şekilde\s+verilmiştir.*?',  # Judgment given in this manner
            r'Taraflar\s+bilgilendirilmiştir.*?',  # Parties have been informed
            r'İtiraz\s+hakkı\s+saklıdır.*?',  # Right to appeal is reserved
            r'Yasal\s+süre\s+içinde\s+.*?',  # Within legal time period
            
            # ADDITIONAL PATTERNS that worked in the debug script
            r'15/\d{2}/\d{4}\s+tarihinde\s+kesin\s+olarak,\s+oyçokluğuyla\s+karar\s+verildi\.',
            r'\(X\)\s+KARŞI\s+OY\s*:\s*.*?(?=\n|$)',
            r'KARAR\s+SONUCU\s*:\s*.*?(?=\n|$)',
            r'kesin\s+olarak,\s+oyçokluğuyla\s+karar\s+verildi\.',
            r'oyçokluğuyla\s+karar\s+verildi\.',
            r'karar\s+verildi\.',
        ]
        
        # Common boilerplate patterns (English + Turkish) - UPDATED to work without line boundaries
        self.common_patterns = [
            # English patterns - REMOVED line boundaries
            r'Copyright.*?',  # Copyright notices
            r'All rights reserved.*?',  # Rights reserved notices
            r'Confidential.*?',  # Confidentiality notices
            r'Proprietary.*?',  # Proprietary notices
            r'Page \d+ of \d+.*?',  # Page numbers
            r'Generated on.*?',  # Generation timestamps
            r'Last updated.*?',  # Last updated timestamps
            r'Version \d+\.\d+.*?',  # Version numbers
            r'Document ID:.*?',  # Document IDs
            r'Reference:.*?',  # Reference numbers
            
            # Turkish patterns - REMOVED line boundaries
            r'Telif\s+hakkı.*?',  # Copyright in Turkish
            r'Tüm\s+haklar\s+saklıdır.*?',  # All rights reserved in Turkish
            r'Gizli.*?',  # Confidential in Turkish
            r'Özel.*?',  # Proprietary in Turkish
            r'Oluşturulma\s+tarihi.*?',  # Created date in Turkish
            r'Son\s+güncelleme.*?',  # Last update in Turkish
            r'Sürüm\s+\d+\.\d+.*?',  # Version in Turkish
            r'Belge\s+kimliği.*?',  # Document ID in Turkish
            r'Referans.*?',  # Reference in Turkish
        ]
        
        # Initialize embedding models
        self._initialize_embedding_models()
        
        if self.debug_mode:
            logger.info(f"DEBUG: Initialized TurkishBoilerplateCleanerProcessor with config: {config}")
            logger.info(f"DEBUG: Turkish legal patterns count: {len(self.turkish_legal_patterns)}")
            logger.info(f"DEBUG: Common patterns count: {len(self.common_patterns)}")
            logger.info(f"DEBUG: Boilerplate columns: {self.boilerplate_columns}")
        
    def _initialize_embedding_models(self):
        """Initialize Turkish-specific embedding models"""
        self.bge_model = None
        self.semhash_model = None
        self.tfidf_vectorizer = None
        
        if self.use_turkish_embeddings:
            if self.embedding_model == 'bge_m3' and BGE_M3_AVAILABLE:
                try:
                    logger.info("Initializing BGE-M3 model for Turkish text processing...")
                    self.bge_model = BGEM3FlagModel('BAAI/bge-m3', use_fp16=True)
                    logger.info("BGE-M3 model initialized successfully")
                except Exception as e:
                    logger.warning(f"Failed to initialize BGE-M3 model: {e}")
                    self.embedding_model = 'tfidf'
                    
            elif self.embedding_model == 'semhash' and SEMHASH_AVAILABLE:
                try:
                    logger.info("Initializing SemHash model for Turkish text processing...")
                    # SemHash handles model initialization internally
                    self.semhash_model = True
                    logger.info("SemHash model initialized successfully")
                except Exception as e:
                    logger.warning(f"Failed to initialize SemHash model: {e}")
                    self.embedding_model = 'tfidf'
                    
            # Fallback to TF-IDF with Turkish stop words
            if self.embedding_model == 'tfidf' or (self.bge_model is None and self.semhash_model is None):
                logger.info("Using TF-IDF with Turkish stop words")
                self.tfidf_vectorizer = TfidfVectorizer(
                    stop_words=list(self.turkish_stopwords),
                    ngram_range=(1, 2),
                    max_features=1000,
                    lowercase=True
                )
                
    def _normalize_turkish_text(self, text: str) -> str:
        """
        Normalize Turkish text by handling special characters and diacritics
        
        Args:
            text (str): Input text
            
        Returns:
            str: Normalized text
        """
        if not self.normalize_turkish_text:
            return text
            
        # Convert to lowercase
        text = text.lower()
        
        # Normalize unicode characters
        text = unicodedata.normalize('NFKC', text)
        
        # Handle Turkish specific characters (optional - depends on use case)
        # text = text.replace('ı', 'i')
        # text = text.replace('ğ', 'g')
        # text = text.replace('ü', 'u')
        # text = text.replace('ş', 's')
        # text = text.replace('ö', 'o')
        # text = text.replace('ç', 'c')
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text.strip())
        
        return text
        
    def _preprocess_turkish_text(self, text: str) -> str:
        """
        Preprocess Turkish text for embedding-based processing
        
        Args:
            text (str): Input text
            
        Returns:
            str: Preprocessed text
        """
        # Normalize text
        text = self._normalize_turkish_text(str(text))
        
        if self.remove_turkish_stopwords and NLTK_AVAILABLE:
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
                            if (len(word) >= 2 and 
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
        Process the DataFrame to clean Turkish boilerplate text
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if self.debug_mode:
            logger.info(f"DEBUG: Processing DataFrame with shape: {df.shape}")
            logger.info(f"DEBUG: DataFrame columns: {list(df.columns)}")
            logger.info(f"DEBUG: Boilerplate columns config: {self.boilerplate_columns}")
        
        if not self.boilerplate_columns:
            logger.warning("DEBUG: No boilerplate columns configured, returning original DataFrame")
            return df
            
        for column, settings in self.boilerplate_columns.items():
            if column not in df.columns:
                logger.warning(f"DEBUG: Column '{column}' not found in DataFrame")
                continue
                
            if self.debug_mode:
                logger.info(f"DEBUG: Processing column '{column}' with settings: {settings}")
                
            # Convert to string type
            df[column] = df[column].astype(str)
            
            # Apply Turkish boilerplate cleaning
            df = self._clean_turkish_boilerplate(df, column, settings)
            
        return df
        
    def _clean_turkish_boilerplate(self, df: pd.DataFrame, column: str, settings: Dict) -> pd.DataFrame:
        """Clean Turkish boilerplate text from data"""
        def clean_text(text):
            try:
                if self.debug_mode:
                    original_text = text
                    original_length = len(text)
                    logger.info(f"DEBUG: Cleaning text with length: {original_length}")
                    logger.info(f"DEBUG: Original text (first 200 chars): {text[:200]}...")
                
                cleaned_text = text
                
                # Remove Turkish legal boilerplate patterns if enabled
                if self.use_legal_patterns:
                    len_before = len(cleaned_text)
                    for pattern in self.turkish_legal_patterns:
                        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
                    if self.debug_mode and len_before != len(cleaned_text):
                        logger.info(f"DEBUG: Turkish legal patterns removed {len_before - len(cleaned_text)} characters")
                
                # Remove common boilerplate patterns
                len_before = len(cleaned_text)
                for pattern in self.common_patterns:
                    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
                if self.debug_mode and len_before != len(cleaned_text):
                    logger.info(f"DEBUG: Common patterns removed {len_before - len(cleaned_text)} characters")
                
                # Remove custom boilerplate patterns if specified
                if 'custom_patterns' in settings:
                    custom_patterns = settings['custom_patterns']
                    len_before = len(cleaned_text)
                    for pattern in custom_patterns:
                        cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
                    if self.debug_mode and len_before != len(cleaned_text):
                        logger.info(f"DEBUG: Custom patterns removed {len_before - len(cleaned_text)} characters")
                
                # Remove duplicate content using Turkish-aware methods
                if settings.get('remove_duplicates', False):
                    len_before = len(cleaned_text)
                    cleaned_text = self._remove_turkish_duplicates(cleaned_text, settings)
                    if self.debug_mode and len_before != len(cleaned_text):
                        logger.info(f"DEBUG: Duplicate removal removed {len_before - len(cleaned_text)} characters")
                
                # Remove header/footer if specified
                if settings.get('remove_header_footer', False):
                    len_before = len(cleaned_text)
                    cleaned_text = self._remove_turkish_header_footer(cleaned_text, settings)
                    if self.debug_mode and len_before != len(cleaned_text):
                        logger.info(f"DEBUG: Header/footer removal removed {len_before - len(cleaned_text)} characters")
                
                # Apply template matching if specified
                if settings.get('template_matching', False):
                    len_before = len(cleaned_text)
                    cleaned_text = self._apply_turkish_template_matching(cleaned_text, settings)
                    if self.debug_mode and len_before != len(cleaned_text):
                        logger.info(f"DEBUG: Template matching removed {len_before - len(cleaned_text)} characters")
                
                # Apply context-aware cleaning if specified
                if settings.get('context_aware_cleaning', False):
                    len_before = len(cleaned_text)
                    cleaned_text = self._apply_turkish_context_aware_cleaning(cleaned_text, settings)
                    if self.debug_mode and len_before != len(cleaned_text):
                        logger.info(f"DEBUG: Context-aware cleaning removed {len_before - len(cleaned_text)} characters")
                
                # Remove extra whitespace
                len_before = len(cleaned_text)
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                if self.debug_mode and len_before != len(cleaned_text):
                    logger.info(f"DEBUG: Whitespace cleanup removed {len_before - len(cleaned_text)} characters")
                
                if self.debug_mode:
                    final_length = len(cleaned_text)
                    reduction = ((original_length - final_length) / original_length * 100) if original_length > 0 else 0
                    logger.info(f"DEBUG: Final result - Original: {original_length}, Cleaned: {final_length}, Reduction: {reduction:.1f}%")
                    logger.info(f"DEBUG: Cleaned text (first 200 chars): {cleaned_text[:200]}...")
                
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning Turkish boilerplate in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df
        
    def _remove_turkish_duplicates(self, text: str, settings: Dict) -> str:
        """Remove duplicate content using Turkish-aware embedding models"""
        try:
            # Split into lines
            lines = text.split('\n')
            
            if len(lines) <= 1:
                return text
                
            # Use appropriate embedding model for duplicate detection
            if self.bge_model and self.embedding_model == 'bge_m3':
                return self._remove_duplicates_with_bge_m3(lines, settings)
            elif self.semhash_model and self.embedding_model == 'semhash':
                return self._remove_duplicates_with_semhash(lines, settings)
            else:
                return self._remove_duplicates_with_tfidf(lines, settings)
                
        except Exception as e:
            logger.warning(f"Error in Turkish duplicate removal: {str(e)}")
            return text
            
    def _remove_duplicates_with_bge_m3(self, lines: List[str], settings: Dict) -> str:
        """Remove duplicates using BGE-M3 embeddings"""
        try:
            if self.bge_model is None:
                logger.warning("BGE-M3 model not available, falling back to TF-IDF")
                return self._remove_duplicates_with_tfidf(lines, settings)
                
            # Preprocess lines
            processed_lines = [self._preprocess_turkish_text(line) for line in lines]
            
            # Get embeddings
            embeddings = self.bge_model.encode(processed_lines, max_length=512)['dense_vecs']
            if not isinstance(embeddings, np.ndarray):
                embeddings = np.array(embeddings)
                
            # Calculate similarity matrix
            similarity_matrix = embeddings @ embeddings.T
            
            # Find duplicates
            duplicates = set()
            for i in range(len(lines)):
                for j in range(i + 1, len(lines)):
                    if similarity_matrix[i, j] > settings.get('similarity_threshold', self.similarity_threshold):
                        duplicates.add(j)
            
            # Remove duplicates
            unique_lines = [line for i, line in enumerate(lines) if i not in duplicates]
            return '\n'.join(unique_lines)
            
        except Exception as e:
            logger.warning(f"Error in BGE-M3 duplicate removal: {str(e)}")
            return '\n'.join(lines)
            
    def _remove_duplicates_with_semhash(self, lines: List[str], settings: Dict) -> str:
        """Remove duplicates using SemHash"""
        try:
            if self.semhash_model is None:
                logger.warning("SemHash model not available, falling back to TF-IDF")
                return self._remove_duplicates_with_tfidf(lines, settings)
                
            # Preprocess lines
            processed_lines = [self._preprocess_turkish_text(line) for line in lines]
            
            # Create records for SemHash
            records = []
            for i, line in enumerate(processed_lines):
                records.append({'text': line, 'id': str(i)})
            
            # Use SemHash.from_records for processing
            semhash = SemHash.from_records(
                records=records,
                columns=['text'],
                use_ann=True
            )
            
            # Perform self-deduplication
            result = semhash.self_deduplicate()
            
            # Get selected (non-duplicate) texts
            selected_texts = [record['text'] for record in result.selected]
            
            # Find which original lines were kept
            kept_indices = set()
            for i, processed_line in enumerate(processed_lines):
                if processed_line in selected_texts:
                    kept_indices.add(i)
            
            # Remove duplicates (keep only the first occurrence of each unique text)
            unique_lines = []
            seen_texts = set()
            for i, line in enumerate(lines):
                processed_line = processed_lines[i]
                if processed_line not in seen_texts:
                    unique_lines.append(line)
                    seen_texts.add(processed_line)
            
            return '\n'.join(unique_lines)
            
        except Exception as e:
            logger.warning(f"Error in SemHash duplicate removal: {str(e)}")
            return '\n'.join(lines)
            
    def _remove_duplicates_with_tfidf(self, lines: List[str], settings: Dict) -> str:
        """Remove duplicates using TF-IDF with Turkish stop words"""
        try:
            if self.tfidf_vectorizer is None:
                logger.warning("TF-IDF vectorizer not available, using simple duplicate removal")
                return self._remove_duplicates_simple(lines, settings)
                
            # Preprocess lines
            processed_lines = [self._preprocess_turkish_text(line) for line in lines]
            
            # Create TF-IDF matrix
            tfidf_matrix = self.tfidf_vectorizer.fit_transform(processed_lines)
            
            # Calculate cosine similarity
            similarity_matrix = cosine_similarity(tfidf_matrix)
            
            # Find duplicates
            duplicates = set()
            for i in range(len(lines)):
                for j in range(i + 1, len(lines)):
                    if similarity_matrix[i, j] > settings.get('similarity_threshold', self.similarity_threshold):
                        duplicates.add(j)
            
            # Remove duplicates
            unique_lines = [line for i, line in enumerate(lines) if i not in duplicates]
            return '\n'.join(unique_lines)
            
        except Exception as e:
            logger.warning(f"Error in TF-IDF duplicate removal: {str(e)}")
            return '\n'.join(lines)
            
    def _remove_duplicates_simple(self, lines: List[str], settings: Dict) -> str:
        """Simple duplicate removal using SequenceMatcher as fallback"""
        try:
            # Use simpler similarity matching
            unique_lines = []
            for line in lines:
                if not any(SequenceMatcher(None, line, existing).ratio() > 
                         settings.get('similarity_threshold', 0.9) 
                         for existing in unique_lines):
                    unique_lines.append(line)
            
            return '\n'.join(unique_lines)
        except Exception as e:
            logger.warning(f"Error in simple duplicate removal: {str(e)}")
            return '\n'.join(lines)
            
    def _remove_turkish_header_footer(self, text: str, settings: Dict) -> str:
        """Remove Turkish headers and footers using smart detection"""
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
            
            # Additional Turkish-specific header/footer detection
            turkish_boilerplate_indicators = [
                't.c.', 'tc', 'türkiye cumhuriyeti', 'sayfa', 'belge no', 'referans no',
                'dosya no', 'kayıt no', 'tarih', 'imza tarihi', 'yürürlük tarihi',
                'yayın tarihi', 'madde', 'bölüm', 'kısım', 'fıkra', 'bent',
                'giriş', 'sonuç', 'karar', 'hüküm', 'gerekçe', 'maddeler', 'ek'
            ]
            
            for line in lines:
                line_lower = line.lower().strip()
                if any(indicator in line_lower for indicator in turkish_boilerplate_indicators):
                    potential_boilerplate.add(line.strip())
            
            # Remove boilerplate lines
            cleaned_lines = [line for line in lines 
                           if line.strip() not in potential_boilerplate]
            
            return '\n'.join(cleaned_lines)
        except Exception as e:
            logger.warning(f"Error in Turkish header/footer removal: {str(e)}")
            return text
            
    def _apply_turkish_template_matching(self, text: str, settings: Dict) -> str:
        """Apply Turkish template matching to identify and remove boilerplate"""
        try:
            if 'turkish_templates' not in settings:
                return text
                
            # Get Turkish template patterns
            templates = settings['turkish_templates']
            
            # Try to match each template
            for template in templates:
                pattern = template.get('pattern')
                if pattern:
                    # Replace template matches with specified replacement
                    replacement = template.get('replacement', '')
                    text = re.sub(pattern, replacement, text, flags=re.MULTILINE | re.IGNORECASE)
            
            return text
        except Exception as e:
            logger.warning(f"Error in Turkish template matching: {str(e)}")
            return text
            
    def _apply_turkish_context_aware_cleaning(self, text: str, settings: Dict) -> str:
        """Apply Turkish context-aware cleaning based on surrounding text"""
        try:
            if 'turkish_context_rules' not in settings:
                return text
                
            # Get Turkish context rules
            context_rules = settings['turkish_context_rules']
            
            # Apply each rule
            for rule in context_rules:
                # Check if context matches
                if re.search(rule.get('context_pattern', ''), text, re.IGNORECASE):
                    # Apply the cleaning action
                    if rule.get('action') == 'remove':
                        text = re.sub(rule.get('pattern', ''), '', text, flags=re.IGNORECASE)
                    elif rule.get('action') == 'replace':
                        text = re.sub(rule.get('pattern', ''), 
                                    rule.get('replacement', ''), text, flags=re.IGNORECASE)
            
            return text
        except Exception as e:
            logger.warning(f"Error in Turkish context-aware cleaning: {str(e)}")
            return text

# Backward compatibility - keep the original class name
BoilerplateCleanerProcessor = TurkishBoilerplateCleanerProcessor 