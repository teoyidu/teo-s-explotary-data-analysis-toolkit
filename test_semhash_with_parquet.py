#!/usr/bin/env python3
"""
Test SemHash Turkish detector with parquet files
"""

import sys
import os
import pandas as pd
import numpy as np
import time
import json
from typing import List, Dict, Any
import logging

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the SemHash Turkish detector
try:
    from src.data_quality.processors.semhash_turkish_detector import SemHashTurkishDetector
    SEMHASH_DETECTOR_AVAILABLE = True
except ImportError as e:
    print(f"Warning: SemHash Turkish detector not available: {e}")
    SEMHASH_DETECTOR_AVAILABLE = False

# Import the original Turkish detector for comparison
try:
    from src.data_quality.processors.duplicate_detector import TurkishDuplicateDetector
    ORIGINAL_DETECTOR_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Original Turkish detector not available: {e}")
    ORIGINAL_DETECTOR_AVAILABLE = False

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_parquet_data(filepath: str) -> pd.DataFrame:
    """Load parquet data and provide basic information"""
    logger.info(f"Loading parquet file: {filepath}")
    
    try:
        df = pd.read_parquet(filepath)
        logger.info(f"Successfully loaded parquet file. Shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}")
        
        # Check for text columns
        text_columns = [col for col in df.columns if 'content' in col.lower()]
        logger.info(f"Text columns found: {text_columns}")
        
        # Show language distribution
        if 'top_language' in df.columns:
            lang_dist = df['top_language'].value_counts()
            logger.info(f"Language distribution:\n{lang_dist.head()}")
        
        return df
        
    except Exception as e:
        logger.error(f"Failed to load parquet file: {e}")
        raise

def test_semhash_with_parquet():
    """Test SemHash Turkish detector with parquet file"""
    print("\n" + "="*80)
    print("TEST: SemHash Turkish Detector with Parquet File")
    print("="*80)
    
    if not SEMHASH_DETECTOR_AVAILABLE:
        print("❌ SemHash Turkish detector not available")
        return False
    
    # Load the parquet data
    parquet_file = "test_data_1.parquet"
    if not os.path.exists(parquet_file):
        print(f"❌ Parquet file not found: {parquet_file}")
        return False
    
    try:
        df = load_parquet_data(parquet_file)
        
        # Filter for Turkish content only
        turkish_df = df[df['top_language'] == '__label__tuk_Latn'].copy()
        logger.info(f"Turkish content rows: {len(turkish_df)}")
        
        if len(turkish_df) == 0:
            print("❌ No Turkish content found in the parquet file")
            return False
        
        # Use the cleaned content column for better results
        text_column = 'sourcefile_content_cleaned'
        if text_column not in turkish_df.columns:
            text_column = 'sourcefile_content'
            logger.warning(f"Using {text_column} instead of cleaned content")
        
        # Remove rows with empty or null text
        turkish_df = turkish_df.dropna(subset=[text_column])
        turkish_df = turkish_df[turkish_df[text_column].str.strip() != '']
        logger.info(f"Valid Turkish content rows: {len(turkish_df)}")
        
        if len(turkish_df) == 0:
            print("❌ No valid Turkish content found after filtering")
            return False
        
        # Sample a subset for testing (to avoid memory issues)
        sample_size = min(1000, len(turkish_df))
        test_df = turkish_df.sample(n=sample_size, random_state=42).copy()
        logger.info(f"Testing with sample of {len(test_df)} rows")
        
        # Configure SemHash detector with stricter settings
        config = {
            'text_column': text_column,
            'similarity_threshold': 0.9,  # More strict threshold
            'action': 'semhash',
            'remove_stopwords': False,    # Less aggressive preprocessing
            'normalize_text': False,      # Less aggressive preprocessing
            'min_word_length': 2,
            'use_multilingual_model': True,
            'use_ann': True,
            'batch_size': 500
        }
        
        logger.info(f"SemHash configuration: {json.dumps(config, indent=2)}")
        
        # Initialize detector
        detector = SemHashTurkishDetector(config)
        
        # INSPECT PREPROCESSING: Show before/after preprocessing
        print(f"\n🔍 PREPROCESSING INSPECTION:")
        print(f"Sample of original texts:")
        for i in range(min(3, len(test_df))):
            original_text = test_df.iloc[i][text_column]
            print(f"  {i+1}. Original: {original_text[:150]}...")
            
            # Show preprocessed version
            preprocessed_text = detector._preprocess_turkish_text(original_text)
            print(f"     Preprocessed: {preprocessed_text[:150]}...")
            print()
        
        # Process DataFrame
        logger.info("Starting SemHash processing...")
        start_time = time.time()
        result_df, stats = detector.process_with_statistics(test_df)
        processing_time = time.time() - start_time
        
        # Display results
        print(f"\n✅ Processing completed in {processing_time:.2f} seconds")
        print(f"📊 Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
        
        # Show duplicate information
        if 'is_duplicate' in result_df.columns:
            duplicate_count = result_df['is_duplicate'].sum()
            unique_count = len(result_df) - duplicate_count
            
            print(f"\n🔍 Duplicate Detection Results:")
            print(f"  Total texts: {len(result_df)}")
            print(f"  Duplicates found: {duplicate_count}")
            print(f"  Unique texts: {unique_count}")
            print(f"  Duplicate percentage: {(duplicate_count / len(result_df) * 100):.2f}%")
            
            # Show some duplicate examples
            if duplicate_count > 0:
                print(f"\n📝 Sample Duplicates:")
                duplicates = result_df[result_df['is_duplicate'] == True]
                for i, (idx, row) in enumerate(duplicates.head(3).iterrows()):
                    text_preview = row[text_column][:100] + "..." if len(row[text_column]) > 100 else row[text_column]
                    print(f"  {i+1}. Row {idx}: {text_preview}")
        
        # Save the modified parquet file
        output_file = "test_data_1_semhash_processed.parquet"
        logger.info(f"Saving processed data to: {output_file}")
        
        # Add processing metadata
        result_df['semhash_processing_timestamp'] = pd.Timestamp.now()
        result_df['semhash_config'] = json.dumps(config)
        result_df['semhash_processing_time_seconds'] = processing_time
        
        result_df.to_parquet(output_file, index=False)
        logger.info(f"✅ Successfully saved processed parquet file: {output_file}")
        
        # Show file size comparison
        original_size = os.path.getsize(parquet_file) / (1024 * 1024)  # MB
        processed_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
        
        print(f"\n💾 File Size Comparison:")
        print(f"  Original: {original_size:.2f} MB")
        print(f"  Processed: {processed_size:.2f} MB")
        print(f"  Size difference: {processed_size - original_size:.2f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_comparison_with_original_detector():
    """Compare SemHash with original Turkish detector on parquet data"""
    print("\n" + "="*80)
    print("COMPARISON: SemHash vs Original Turkish Detector")
    print("="*80)
    
    if not SEMHASH_DETECTOR_AVAILABLE or not ORIGINAL_DETECTOR_AVAILABLE:
        print("❌ Both detectors not available for comparison")
        return False
    
    # Load a smaller sample for comparison
    parquet_file = "test_data_1.parquet"
    try:
        df = pd.read_parquet(parquet_file)
        turkish_df = df[df['top_language'] == '__label__tuk_Latn'].copy()
        
        # Use smaller sample for comparison
        test_df = turkish_df.sample(n=500, random_state=42).copy()
        test_df = test_df.dropna(subset=['sourcefile_content_cleaned'])
        test_df = test_df[test_df['sourcefile_content_cleaned'].str.strip() != '']
        
        text_column = 'sourcefile_content_cleaned'
        
        print(f"Testing with {len(test_df)} Turkish texts")
        
        # Test SemHash
        print("\n--- SemHash Detector ---")
        semhash_config = {
            'text_column': text_column,
            'similarity_threshold': 0.9,  # More strict threshold
            'action': 'semhash',
            'remove_stopwords': False,    # Less aggressive preprocessing
            'normalize_text': False,      # Less aggressive preprocessing
            'min_word_length': 2,
            'use_multilingual_model': True,
            'use_ann': True
        }
        
        semhash_detector = SemHashTurkishDetector(semhash_config)
        semhash_start = time.time()
        semhash_result, semhash_stats = semhash_detector.process_with_statistics(test_df)
        semhash_time = time.time() - semhash_start
        
        print(f"SemHash processing time: {semhash_time:.2f} seconds")
        print(f"SemHash duplicates found: {semhash_stats.get('duplicate_count', 0)}")
        
        # Test original detector
        print("\n--- Original Detector ---")
        original_config = {
            'text_column': text_column,
            'similarity_threshold': 0.8,
            'action': 'mark',
            'remove_stopwords': True,
            'normalize_text': True,
            'min_word_length': 2
        }
        
        original_detector = TurkishDuplicateDetector(original_config)
        original_start = time.time()
        original_result = original_detector.process(test_df)
        original_time = time.time() - original_start
        
        original_duplicates = original_result['is_duplicate'].sum() if 'is_duplicate' in original_result.columns else 0
        
        print(f"Original processing time: {original_time:.2f} seconds")
        print(f"Original duplicates found: {original_duplicates}")
        
        # Comparison summary
        print("\n--- Comparison Summary ---")
        if original_time > 0:
            speed_improvement = original_time / semhash_time
            print(f"Speed improvement: {speed_improvement:.2f}x faster with SemHash")
        
        print(f"SemHash duplicates: {semhash_stats.get('duplicate_count', 0)}")
        print(f"Original duplicates: {original_duplicates}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Comparison test failed: {e}")
        return False

def analyze_duplicate_patterns():
    """Analyze patterns in detected duplicates"""
    print("\n" + "="*80)
    print("ANALYSIS: Duplicate Patterns")
    print("="*80)
    
    processed_file = "test_data_1_semhash_processed.parquet"
    if not os.path.exists(processed_file):
        print(f"❌ Processed file not found: {processed_file}")
        return False
    
    try:
        df = pd.read_parquet(processed_file)
        
        if 'is_duplicate' not in df.columns:
            print("❌ No duplicate information found in processed file")
            return False
        
        duplicates = df[df['is_duplicate'] == True]
        unique_texts = df[df['is_duplicate'] == False]
        
        print(f"📊 Duplicate Analysis:")
        print(f"  Total texts: {len(df)}")
        print(f"  Duplicates: {len(duplicates)}")
        print(f"  Unique texts: {len(unique_texts)}")
        
        # Analyze text length patterns
        if 'sourcefile_content_cleaned' in df.columns:
            text_col = 'sourcefile_content_cleaned'
        else:
            text_col = 'sourcefile_content'
        
        # Calculate text length for analysis
        df['text_length'] = df[text_col].str.len()
        duplicates['text_length'] = duplicates[text_col].str.len()
        unique_texts['text_length'] = unique_texts[text_col].str.len()
        
        print(f"\n📏 Text Length Analysis:")
        print(f"  Average length (all): {df['text_length'].mean():.0f} characters")
        print(f"  Average length (duplicates): {duplicates['text_length'].mean():.0f} characters")
        print(f"  Average length (unique): {unique_texts['text_length'].mean():.0f} characters")
        
        # Analyze quality patterns
        if 'quality_category' in df.columns:
            print(f"\n🏆 Quality Analysis:")
            quality_dist = df['quality_category'].value_counts()
            print(f"  Quality distribution (all):\n{quality_dist}")
            
            duplicate_quality = duplicates['quality_category'].value_counts()
            print(f"  Quality distribution (duplicates):\n{duplicate_quality}")
        
        # Show some example duplicates
        if len(duplicates) > 0:
            print(f"\n📝 Example Duplicates:")
            for i, (idx, row) in enumerate(duplicates.head(5).iterrows()):
                text_preview = row[text_col][:150] + "..." if len(row[text_col]) > 150 else row[text_col]
                print(f"  {i+1}. Row {idx} (Length: {row['text_length']}):")
                print(f"     {text_preview}")
                print()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test runner"""
    print("🚀 Starting SemHash Turkish Detector Parquet Test Suite")
    print("="*80)
    
    tests = [
        test_semhash_with_parquet,
        test_comparison_with_original_detector,
        analyze_duplicate_patterns
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            logger.error(f"❌ Test {test.__name__} failed with exception: {e}")
    
    # Print summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Tests passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        print("📁 Check 'test_data_1_semhash_processed.parquet' for the processed results")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    exit(main()) 