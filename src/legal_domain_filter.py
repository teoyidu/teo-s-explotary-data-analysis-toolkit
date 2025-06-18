"""
Legal Domain Filtering Processor
Uses BERTurk-Legal model to identify legal domain content
"""

import logging
from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import BooleanType
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

class LegalDomainFilter:
    """Processor for filtering legal domain content using BERTurk-Legal model"""
    
    def __init__(self):
        """Initialize the BERTurk-Legal model and tokenizer"""
        self.model_name = "KocLab-Bilkent/BERTurk-Legal"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.model.eval()  # Set model to evaluation mode
        
        # Legal domain keywords in Turkish
        self.legal_keywords = [
            "Yönetmelik", "Kanun", "Madde", "Fıkra", "Resmî Gazete",
            "Tüzük", "Kararname", "Genelge", "Tebliğ", "Yönerge",
            "Talimat", "Sözleşme", "Dilekçe", "İstinaf", "Temyiz",
            "Dava", "Mahkeme", "Savcı", "Hakim", "Avukat"
        ]
    
    def _is_legal_domain(self, text: str) -> bool:
        """
        Check if the text belongs to legal domain using both keyword matching and BERT model
        
        Args:
            text (str): Input text to check
            
        Returns:
            bool: True if text is legal domain, False otherwise
        """
        if not text or not isinstance(text, str):
            return False
            
        # First check for legal keywords
        text_lower = text.lower()
        if any(keyword.lower() in text_lower for keyword in self.legal_keywords):
            return True
            
        # If no keywords found, use BERT model
        try:
            inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512)
            with torch.no_grad():
                outputs = self.model(**inputs)
                predictions = torch.softmax(outputs.logits, dim=1)
                legal_prob = predictions[0][1].item()  # Assuming 1 is the legal class
                return legal_prob > 0.5
        except Exception as e:
            logger.error(f"Error in BERT model prediction: {str(e)}")
            return False
    
    def process(self, df: DataFrame, text_column: str) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Process the DataFrame to filter legal domain content
        
        Args:
            df (DataFrame): Input DataFrame
            text_column (str): Name of the column containing text to analyze
            
        Returns:
            Tuple[DataFrame, Dict[str, Any]]: Processed DataFrame and statistics
        """
        # Register UDF for legal domain detection
        is_legal_udf = udf(self._is_legal_domain, BooleanType())
        
        # Add legal domain flag column
        df = df.withColumn("is_legal_domain", is_legal_udf(col(text_column)))
        
        # Calculate statistics
        total_count = df.count()
        legal_count = df.filter(col("is_legal_domain")).count()
        
        stats = {
            "total_documents": total_count,
            "legal_documents": legal_count,
            "legal_percentage": (legal_count / total_count * 100) if total_count > 0 else 0
        }
        
        logger.info(f"Legal domain filtering completed. Found {legal_count} legal documents out of {total_count} total documents.")
        
        return df, stats 