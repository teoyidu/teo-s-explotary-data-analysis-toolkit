#!/usr/bin/env python3
"""
Test BGE-M3 Turkish detector with parquet files
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

# Import the BGE-M3 Turkish detector
try:
    from src.data_quality.processors.bge_m3_turkish_detector import BGEM3TurkishDetector
    BGE_M3_DETECTOR_AVAILABLE = True
except ImportError as e:
    print(f"Warning: BGE-M3 Turkish detector not available: {e}")
    BGE_M3_DETECTOR_AVAILABLE = False

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

def test_bge_m3_with_parquet():
    """Test BGE-M3 Turkish detector with parquet file"""
    print("\n" + "="*80)
    print("TEST: BGE-M3 Turkish Detector with Parquet File")
    print("="*80)
    
    if not BGE_M3_DETECTOR_AVAILABLE:
        print("❌ BGE-M3 Turkish detector not available")
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
        sample_size = min(500, len(turkish_df))  # Smaller sample for BGE-M3
        test_df = turkish_df.sample(n=sample_size, random_state=42).copy()
        logger.info(f"Testing with sample of {len(test_df)} rows")
        
        # Configure BGE-M3 detector with different retrieval modes
        configs = [
            {
                'name': 'Dense Mode',
                'config': {
                    'text_column': text_column,
                    'similarity_threshold': 0.85,
                    'action': 'bge_m3',
                    'remove_stopwords': False,
                    'normalize_text': False,
                    'min_word_length': 2,
                    'use_fp16': True,
                    'max_length': 512,
                    'batch_size': 32,
                    'retrieval_mode': 'dense'
                }
            },
            {
                'name': 'Sparse Mode',
                'config': {
                    'text_column': text_column,
                    'similarity_threshold': 0.3,  # Lower threshold for sparse
                    'action': 'bge_m3',
                    'remove_stopwords': False,
                    'normalize_text': False,
                    'min_word_length': 2,
                    'use_fp16': True,
                    'max_length': 512,
                    'batch_size': 32,
                    'retrieval_mode': 'sparse'
                }
            },
            {
                'name': 'Hybrid Mode',
                'config': {
                    'text_column': text_column,
                    'similarity_threshold': 0.8,
                    'action': 'bge_m3',
                    'remove_stopwords': False,
                    'normalize_text': False,
                    'min_word_length': 2,
                    'use_fp16': True,
                    'max_length': 512,
                    'batch_size': 32,
                    'retrieval_mode': 'hybrid',
                    'hybrid_weights': [0.4, 0.2, 0.4]
                }
            }
        ]
        
        best_result = None
        best_config = None
        best_duplicate_rate = 0
        
        for config_info in configs:
            print(f"\n🔍 Testing {config_info['name']}...")
            
            try:
                # Initialize detector
                detector = BGEM3TurkishDetector(config_info['config'])
                
                # INSPECT PREPROCESSING: Show before/after preprocessing
                print(f"Sample of original texts:")
                for i in range(min(2, len(test_df))):
                    original_text = test_df.iloc[i][text_column]
                    print(f"  {i+1}. Original: {original_text[:100]}...")
                    
                    # Show preprocessed version
                    preprocessed_text = detector._preprocess_turkish_text(original_text)
                    print(f"     Preprocessed: {preprocessed_text[:100]}...")
                    print()
                
                # Process DataFrame
                logger.info(f"Starting BGE-M3 processing with {config_info['name']}...")
                start_time = time.time()
                result_df, stats = detector.process_with_statistics(test_df)
                processing_time = time.time() - start_time
                
                # Display results
                print(f"✅ {config_info['name']} completed in {processing_time:.2f} seconds")
                print(f"📊 Statistics:")
                for key, value in stats.items():
                    print(f"  {key}: {value}")
                
                # Show duplicate information
                if 'is_duplicate' in result_df.columns:
                    duplicate_count = result_df['is_duplicate'].sum()
                    unique_count = len(result_df) - duplicate_count
                    duplicate_rate = (duplicate_count / len(result_df) * 100)
                    
                    print(f"\n🔍 Duplicate Detection Results:")
                    print(f"  Total texts: {len(result_df)}")
                    print(f"  Duplicates found: {duplicate_count}")
                    print(f"  Unique texts: {unique_count}")
                    print(f"  Duplicate percentage: {duplicate_rate:.2f}%")
                    
                    # Track best result (reasonable duplicate rate)
                    if 5 <= duplicate_rate <= 50:  # Reasonable range
                        if best_result is None or abs(duplicate_rate - 20) < abs(best_duplicate_rate - 20):
                            best_result = result_df
                            best_config = config_info
                            best_duplicate_rate = duplicate_rate
                    
                    # Show some duplicate examples
                    if duplicate_count > 0:
                        print(f"\n📝 Sample Duplicates:")
                        duplicates = result_df[result_df['is_duplicate'] == True]
                        for i, (idx, row) in enumerate(duplicates.head(3).iterrows()):
                            text_preview = row[text_column][:100] + "..." if len(row[text_column]) > 100 else row[text_column]
                            print(f"  {i+1}. Row {idx}: {text_preview}")
                
            except Exception as e:
                logger.error(f"❌ {config_info['name']} failed: {e}")
                continue
        
        # Use the best result or the last successful one
        if best_result is not None and best_config is not None:
            result_df = best_result
            print(f"\n🎯 Using best configuration: {best_config['name']} (duplicate rate: {best_duplicate_rate:.2f}%)")
        else:
            print(f"\n⚠️  No optimal configuration found, using last successful result")
        
        # Save the modified parquet file
        output_file = "test_data_1_bge_m3_processed.parquet"
        logger.info(f"Saving processed data to: {output_file}")
        
        # Add processing metadata
        result_df['bge_m3_processing_timestamp'] = pd.Timestamp.now()
        if best_config is not None:
            result_df['bge_m3_config'] = json.dumps(best_config['config'])
        else:
            result_df['bge_m3_config'] = json.dumps(configs[-1]['config'])
        result_df['bge_m3_processing_time_seconds'] = processing_time
        
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
    """Compare BGE-M3 with original Turkish detector on parquet data"""
    print("\n" + "="*80)
    print("COMPARISON: BGE-M3 vs Original Turkish Detector")
    print("="*80)
    
    if not BGE_M3_DETECTOR_AVAILABLE or not ORIGINAL_DETECTOR_AVAILABLE:
        print("❌ Both detectors not available for comparison")
        return False
    
    # Load a smaller sample for comparison
    parquet_file = "test_data_1.parquet"
    try:
        df = pd.read_parquet(parquet_file)
        turkish_df = df[df['top_language'] == '__label__tuk_Latn'].copy()
        
        # Use smaller sample for comparison
        test_df = turkish_df.sample(n=200, random_state=42).copy()
        test_df = test_df.dropna(subset=['sourcefile_content_cleaned'])
        test_df = test_df[test_df['sourcefile_content_cleaned'].str.strip() != '']
        
        text_column = 'sourcefile_content_cleaned'
        
        print(f"Testing with {len(test_df)} Turkish texts")
        
        # Test BGE-M3
        print("\n--- BGE-M3 Detector (Dense Mode) ---")
        bge_config = {
            'text_column': text_column,
            'similarity_threshold': 0.85,
            'action': 'bge_m3',
            'remove_stopwords': False,
            'normalize_text': False,
            'min_word_length': 2,
            'use_fp16': True,
            'max_length': 512,
            'batch_size': 32,
            'retrieval_mode': 'dense'
        }
        
        bge_detector = BGEM3TurkishDetector(bge_config)
        bge_start = time.time()
        bge_result, bge_stats = bge_detector.process_with_statistics(test_df)
        bge_time = time.time() - bge_start
        
        print(f"BGE-M3 processing time: {bge_time:.2f} seconds")
        print(f"BGE-M3 duplicates found: {bge_stats.get('duplicate_count', 0)}")
        print(f"BGE-M3 duplicate groups: {bge_stats.get('duplicate_groups', 0)}")
        
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
            speed_improvement = original_time / bge_time
            print(f"Speed comparison: {speed_improvement:.2f}x faster with BGE-M3")
        
        print(f"BGE-M3 duplicates: {bge_stats.get('duplicate_count', 0)}")
        print(f"Original duplicates: {original_duplicates}")
        print(f"BGE-M3 duplicate rate: {bge_stats.get('duplicate_percentage', 0):.2f}%")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Comparison test failed: {e}")
        return False

def analyze_duplicate_patterns():
    """Analyze patterns in detected duplicates"""
    print("\n" + "="*80)
    print("ANALYSIS: Duplicate Patterns")
    print("="*80)
    
    processed_file = "test_data_1_bge_m3_processed.parquet"
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
    print("🚀 Starting BGE-M3 Turkish Detector Parquet Test Suite")
    print("="*80)
    
    tests = [
        test_bge_m3_with_parquet,
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
        print("📁 Check 'test_data_1_bge_m3_processed.parquet' for the processed results")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    exit(main()) 