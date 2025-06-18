"""
Legal Domain Filtering Processor
Uses BERTurk-Legal model to identify legal domain content
"""

import logging
import os
from typing import Dict, List, Tuple, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import BooleanType, FloatType
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from functools import lru_cache
import json

from ..exceptions import LegalDomainError, ModelLoadError, InferenceError

logger = logging.getLogger(__name__)

class LegalDomainFilter:
    """Processor for filtering legal domain content using BERTurk-Legal model"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the BERTurk-Legal model and tokenizer
        
        Args:
            config (Dict[str, Any]): Configuration dictionary containing:
                - model_name: Name of the model to use (default: KocLab-Bilkent/BERTurk-Legal)
                - cache_dir: Directory to cache the model (default: ./model_cache)
                - threshold: Probability threshold for legal domain classification (default: 0.5)
                - batch_size: Batch size for processing (default: 32)
                - device: Device to use for inference (default: auto)
        """
        self.config = config
        self.model_name = config.get('model_name', 'KocLab-Bilkent/BERTurk-Legal')
        self.cache_dir = config.get('cache_dir', './model_cache')
        self.threshold = config.get('threshold', 0.5)
        self.batch_size = config.get('batch_size', 32)
        
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Determine device
        self.device = config.get('device', 'cuda' if torch.cuda.is_available() else 'cpu')
        
        # Initialize model and tokenizer
        try:
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name,
                cache_dir=self.cache_dir,
                local_files_only=False
            )
            self.model = AutoModelForSequenceClassification.from_pretrained(
                self.model_name,
                cache_dir=self.cache_dir,
                local_files_only=False
            )
            self.model.to(self.device)
            self.model.eval()  # Set model to evaluation mode
        except Exception as e:
            logger.error(f"Failed to load model: {str(e)}")
            raise ModelLoadError(f"Failed to load model: {str(e)}")
        
        # Legal domain keywords in Turkish
        self.legal_keywords = [
            "Yönetmelik", "Kanun", "Madde", "Fıkra", "Resmî Gazete",
            "Tüzük", "Kararname", "Genelge", "Tebliğ", "Yönerge",
            "Talimat", "Sözleşme", "Dilekçe", "İstinaf", "Temyiz",
            "Dava", "Mahkeme", "Savcı", "Hakim", "Avukat"
        ]
        
        # Initialize cache for model predictions
        self._prediction_cache = {}
    
    @lru_cache(maxsize=1000)
    def _is_legal_domain(self, text: str) -> Tuple[bool, float]:
        """
        Check if the text belongs to legal domain using both keyword matching and BERT model
        
        Args:
            text (str): Input text to check
            
        Returns:
            Tuple[bool, float]: (is_legal, probability)
        """
        if not text or not isinstance(text, str):
            return False, 0.0
            
        # Check cache first
        if text in self._prediction_cache:
            return self._prediction_cache[text]
            
        # First check for legal keywords
        text_lower = text.lower()
        if any(keyword.lower() in text_lower for keyword in self.legal_keywords):
            self._prediction_cache[text] = (True, 1.0)
            return True, 1.0
            
        # If no keywords found, use BERT model
        try:
            inputs = self.tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=512,
                padding=True
            ).to(self.device)
            
            with torch.no_grad():
                outputs = self.model(**inputs)
                predictions = torch.softmax(outputs.logits, dim=1)
                legal_prob = predictions[0][1].item()  # Assuming 1 is the legal class
                
                result = (legal_prob > self.threshold, legal_prob)
                self._prediction_cache[text] = result
                return result
                
        except Exception as e:
            logger.error(f"Error in BERT model prediction: {str(e)}")
            raise InferenceError(f"Model inference failed: {str(e)}")
    
    def process(self, df: DataFrame, text_column: str) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Process the DataFrame to filter legal domain content
        
        Args:
            df (DataFrame): Input DataFrame
            text_column (str): Name of the column containing text to analyze
            
        Returns:
            Tuple[DataFrame, Dict[str, Any]]: Processed DataFrame and statistics
        """
        if text_column not in df.columns:
            raise ValueError(f"Text column '{text_column}' not found in DataFrame")
        
        # Register UDFs for legal domain detection
        is_legal_udf = udf(lambda x: self._is_legal_domain(x)[0], BooleanType())
        legal_prob_udf = udf(lambda x: self._is_legal_domain(x)[1], FloatType())
        
        # Add legal domain flag and probability columns
        df = df.withColumn("is_legal_domain", is_legal_udf(col(text_column)))
        df = df.withColumn("legal_probability", legal_prob_udf(col(text_column)))
        
        # Calculate statistics
        total_count = df.count()
        legal_count = df.filter(col("is_legal_domain")).count()
        
        stats = {
            "total_documents": total_count,
            "legal_documents": legal_count,
            "legal_percentage": (legal_count / total_count * 100) if total_count > 0 else 0,
            "model_name": self.model_name,
            "threshold": self.threshold,
            "device": self.device
        }
        
        logger.info(
            f"Legal domain filtering completed. Found {legal_count} legal documents "
            f"out of {total_count} total documents."
        )
        
        return df, stats 