#!/usr/bin/env python3
"""
Comprehensive test suite for SemHash Turkish duplicate detector
"""

import sys
import os
import pandas as pd
import numpy as np
import time
import json
from typing import List, Dict, Any

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

def get_basic_turkish_texts():
    """Basic Turkish texts with duplicates"""
    return [
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

def get_legal_turkish_texts():
    """Turkish legal texts with duplicates"""
    return [
        "Bu sözleşme taraflar arasında imzalanmıştır.",
        "Bu sözleşme taraflar arasında imzalanmıştır.",  # Exact duplicate
        "Bu sözleşme, taraflar arasında imzalanmıştır.",  # Near duplicate with comma
        "Sözleşme taraflar arasında imzalanmıştır.",  # Similar meaning
        "Taraflar arasında bu sözleşme imzalanmıştır.",  # Different word order
        "Bu sözleşme taraflar arasında imzalanmıştır.",  # Another exact duplicate
        "Mahkeme kararı kesinleşmiştir.",
        "Mahkeme kararı kesinleşmiştir.",  # Exact duplicate
        "Mahkeme kararı kesinleşmiştir.",  # Another exact duplicate
        "Yargıtay kararı kesinleşmiştir.",  # Similar legal text
    ]

def test_basic_functionality():
    """Test basic functionality with simple Turkish texts"""
    print("\n" + "="*60)
    print("TEST 1: Basic Functionality")
    print("="*60)
    
    if not SEMHASH_DETECTOR_AVAILABLE:
        print("❌ SemHash Turkish detector not available")
        return False
        
    # Create test data
    texts = get_basic_turkish_texts()
    df = pd.DataFrame({'text': texts})
    
    print(f"Original DataFrame shape: {df.shape}")
    print("Sample texts:")
    for i, text in enumerate(texts[:3]):
        print(f"  {i}: {text}")
    
    # Configure detector
    config = {
        'text_column': 'text',
        'similarity_threshold': 0.8,
        'action': 'semhash',
        'remove_stopwords': True,
        'normalize_text': True,
        'min_word_length': 2,
        'use_multilingual_model': True,
        'use_ann': True
    }
    
    try:
        # Initialize detector
        detector = SemHashTurkishDetector(config)
        
        # Process DataFrame
        start_time = time.time()
        result_df, stats = detector.process_with_statistics(df)
        processing_time = time.time() - start_time
        
        # Display results
        print(f"\nProcessing time: {processing_time:.2f} seconds")
        print(f"Statistics: {json.dumps(stats, indent=2)}")
        
        print(f"\nResult DataFrame shape: {result_df.shape}")
        print("Duplicate detection results:")
        if 'is_duplicate' in result_df.columns:
            duplicate_count = result_df['is_duplicate'].sum()
            print(f"  Duplicates found: {duplicate_count}")
            print(f"  Unique texts: {len(result_df) - duplicate_count}")
            
            # Show duplicate groups
            if 'duplicate_group' in result_df.columns:
                duplicate_groups = result_df[result_df['duplicate_group'] >= 0].groupby('duplicate_group')
                print(f"  Number of duplicate groups: {len(duplicate_groups)}")
                
                for group_id, group in duplicate_groups:
                    print(f"    Group {group_id}: {len(group)} texts")
                    for idx, row in group.iterrows():
                        print(f"      Row {idx}: {row['text'][:50]}...")
        else:
            print("  No duplicate information found")
            
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_legal_texts():
    """Test with Turkish legal texts"""
    print("\n" + "="*60)
    print("TEST 2: Legal Texts")
    print("="*60)
    
    if not SEMHASH_DETECTOR_AVAILABLE:
        print("❌ SemHash Turkish detector not available")
        return False
        
    # Create test data
    texts = get_legal_turkish_texts()
    df = pd.DataFrame({'text': texts})
    
    print(f"Original DataFrame shape: {df.shape}")
    print("Sample legal texts:")
    for i, text in enumerate(texts[:3]):
        print(f"  {i}: {text}")
    
    # Configure detector
    config = {
        'text_column': 'text',
        'similarity_threshold': 0.7,  # Lower threshold for legal texts
        'action': 'semhash',
        'remove_stopwords': True,
        'normalize_text': True,
        'min_word_length': 2,
        'use_multilingual_model': True,
        'use_ann': True
    }
    
    try:
        # Initialize detector
        detector = SemHashTurkishDetector(config)
        
        # Process DataFrame
        start_time = time.time()
        result_df, stats = detector.process_with_statistics(df)
        processing_time = time.time() - start_time
        
        # Display results
        print(f"\nProcessing time: {processing_time:.2f} seconds")
        print(f"Statistics: {json.dumps(stats, indent=2)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def test_comparison_with_original():
    """Compare SemHash with original Turkish detector"""
    print("\n" + "="*60)
    print("TEST 3: Comparison with Original Detector")
    print("="*60)
    
    if not SEMHASH_DETECTOR_AVAILABLE or not ORIGINAL_DETECTOR_AVAILABLE:
        print("❌ Both detectors not available for comparison")
        return False
        
    # Create test data
    texts = get_basic_turkish_texts()
    df = pd.DataFrame({'text': texts})
    
    print(f"Test DataFrame shape: {df.shape}")
    
    # Test SemHash detector
    print("\n--- SemHash Detector ---")
    semhash_config = {
        'text_column': 'text',
        'similarity_threshold': 0.8,
        'action': 'semhash',
        'remove_stopwords': True,
        'normalize_text': True,
        'min_word_length': 2,
        'use_multilingual_model': True,
        'use_ann': True
    }
    
    try:
        semhash_detector = SemHashTurkishDetector(semhash_config)
        semhash_start = time.time()
        semhash_result, semhash_stats = semhash_detector.process_with_statistics(df)
        semhash_time = time.time() - semhash_start
        
        print(f"SemHash processing time: {semhash_time:.2f} seconds")
        print(f"SemHash statistics: {json.dumps(semhash_stats, indent=2)}")
        
    except Exception as e:
        print(f"❌ SemHash test failed: {e}")
        return False
    
    # Test original detector
    print("\n--- Original Detector ---")
    original_config = {
        'text_column': 'text',
        'similarity_threshold': 0.8,
        'action': 'mark',
        'remove_stopwords': True,
        'normalize_text': True,
        'min_word_length': 2
    }
    
    try:
        original_detector = TurkishDuplicateDetector(original_config)
        original_start = time.time()
        original_result = original_detector.process(df)
        original_time = time.time() - original_start
        
        original_duplicates = original_result['is_duplicate'].sum() if 'is_duplicate' in original_result.columns else 0
        
        print(f"Original processing time: {original_time:.2f} seconds")
        print(f"Original duplicates found: {original_duplicates}")
        
    except Exception as e:
        print(f"❌ Original detector test failed: {e}")
        return False
    
    # Compare results
    print("\n--- Comparison ---")
    print(f"Speed improvement: {original_time / semhash_time:.2f}x faster with SemHash")
    print(f"SemHash duplicates: {semhash_stats.get('duplicate_count', 0)}")
    print(f"Original duplicates: {original_duplicates}")
    
    return True

def test_performance_scaling():
    """Test performance with larger datasets"""
    print("\n" + "="*60)
    print("TEST 4: Performance Scaling")
    print("="*60)
    
    if not SEMHASH_DETECTOR_AVAILABLE:
        print("❌ SemHash Turkish detector not available")
        return False
        
    # Create larger test dataset
    base_texts = get_basic_turkish_texts()
    large_texts = []
    
    # Repeat texts to create larger dataset
    for i in range(50):  # 50 repetitions
        for text in base_texts:
            large_texts.append(f"{text} (copy {i})")
    
    df = pd.DataFrame({'text': large_texts})
    
    print(f"Large test DataFrame shape: {df.shape}")
    
    # Configure detector
    config = {
        'text_column': 'text',
        'similarity_threshold': 0.8,
        'action': 'semhash',
        'remove_stopwords': True,
        'normalize_text': True,
        'min_word_length': 2,
        'use_multilingual_model': True,
        'use_ann': True,
        'batch_size': 1000
    }
    
    try:
        # Initialize detector
        detector = SemHashTurkishDetector(config)
        
        # Process DataFrame
        start_time = time.time()
        result_df, stats = detector.process_with_statistics(df)
        processing_time = time.time() - start_time
        
        # Display results
        print(f"\nProcessing time: {processing_time:.2f} seconds")
        print(f"Texts per second: {len(df) / processing_time:.2f}")
        print(f"Statistics: {json.dumps(stats, indent=2)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

def main():
    """Main test runner"""
    print("🚀 Starting SemHash Turkish Detector Test Suite")
    print("="*60)
    
    tests = [
        test_basic_functionality,
        test_legal_texts,
        test_comparison_with_original,
        test_performance_scaling
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
    
    # Print summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    print(f"Tests passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print("\n⚠️  Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    exit(main()) 