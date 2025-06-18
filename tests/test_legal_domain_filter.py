"""
Tests for the LegalDomainFilter class
"""

import unittest
import pandas as pd
from pyspark.sql import SparkSession
from src.data_quality.utils.legal_domain_filter import LegalDomainFilter
from src.data_quality.exceptions import ModelLoadError, InferenceError

class TestLegalDomainFilter(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        cls.spark = SparkSession.builder \
            .appName("TestLegalDomainFilter") \
            .master("local[2]") \
            .getOrCreate()
            
        cls.config = {
            "model_name": "KocLab-Bilkent/BERTurk-Legal",
            "cache_dir": "./test_model_cache",
            "threshold": 0.5,
            "batch_size": 32,
            "device": "cpu"
        }
        
        cls.filter = LegalDomainFilter(cls.config)
        
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        cls.spark.stop()
        
    def test_initialization(self):
        """Test filter initialization"""
        self.assertEqual(self.filter.model_name, self.config["model_name"])
        self.assertEqual(self.filter.threshold, self.config["threshold"])
        self.assertEqual(self.filter.batch_size, self.config["batch_size"])
        self.assertEqual(self.filter.device, self.config["device"])
        
    def test_legal_keywords(self):
        """Test legal keyword detection"""
        # Test with legal keywords
        legal_text = "Bu bir Yönetmelik metnidir."
        is_legal, prob = self.filter._is_legal_domain(legal_text)
        self.assertTrue(is_legal)
        self.assertEqual(prob, 1.0)
        
        # Test with non-legal text
        non_legal_text = "Bu bir normal metindir."
        is_legal, prob = self.filter._is_legal_domain(non_legal_text)
        self.assertFalse(is_legal)
        self.assertLess(prob, self.filter.threshold)
        
    def test_process_dataframe(self):
        """Test processing a DataFrame"""
        # Create test data
        data = [
            ("Bu bir Yönetmelik metnidir.",),
            ("Bu bir normal metindir.",),
            ("Bu bir Kanun metnidir.",)
        ]
        df = self.spark.createDataFrame(data, ["text"])
        
        # Process DataFrame
        result_df, stats = self.filter.process(df, "text")
        
        # Check results
        self.assertIn("is_legal_domain", result_df.columns)
        self.assertIn("legal_probability", result_df.columns)
        
        # Count legal documents
        legal_count = result_df.filter("is_legal_domain = true").count()
        self.assertEqual(legal_count, 2)  # Two documents with legal keywords
        
        # Check statistics
        self.assertEqual(stats["total_documents"], 3)
        self.assertEqual(stats["legal_documents"], 2)
        self.assertAlmostEqual(stats["legal_percentage"], 66.67, places=2)
        
    def test_invalid_text_column(self):
        """Test handling of invalid text column"""
        df = self.spark.createDataFrame([("test",)], ["wrong_column"])
        with self.assertRaises(ValueError):
            self.filter.process(df, "text")
            
    def test_empty_text(self):
        """Test handling of empty text"""
        is_legal, prob = self.filter._is_legal_domain("")
        self.assertFalse(is_legal)
        self.assertEqual(prob, 0.0)
        
        is_legal, prob = self.filter._is_legal_domain(None)
        self.assertFalse(is_legal)
        self.assertEqual(prob, 0.0)

if __name__ == "__main__":
    unittest.main() 