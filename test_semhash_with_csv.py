#!/usr/bin/env python3
"""
Comprehensive test script for SemHash Turkish detector using CSV test data
"""

import pandas as pd
import sys
import os
import time
import json
from typing import Dict, Any, List

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def load_test_data(csv_file: str = "turkish_test_data.csv") -> pd.DataFrame | None:
    """Load test data from CSV file"""
    try:
        df = pd.read_csv(csv_file)
        print(f"✅ Loaded test data: {len(df)} texts from {csv_file}")
        return df
    except FileNotFoundError:
        print(f"❌ Test data file {csv_file} not found")
        return None
    except Exception as e:
        print(f"❌ Error loading test data: {e}")
        return None

def analyze_test_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze the test data structure"""
    analysis = {
        'total_texts': len(df),
        'categories': df['category'].value_counts().to_dict(),
        'expected_duplicates': df['expected_duplicates'].value_counts().to_dict(),
        'sample_texts': df['text'].head(5).tolist()
    }
    
    print(f"\n📊 Test Data Analysis:")
    print(f"  Total texts: {analysis['total_texts']}")
    print(f"  Categories: {analysis['categories']}")
    print(f"  Expected duplicates distribution: {analysis['expected_duplicates']}")
    print(f"  Sample texts:")
    for i, text in enumerate(analysis['sample_texts']):
        print(f"    {i}: {text[:50]}...")
    
    return analysis

def test_semhash_detector(df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    """Test SemHash Turkish detector with given configuration"""
    print(f"\n🔧 Testing SemHash with config: {config}")
    
    try:
        from src.data_quality.processors.semhash_turkish_detector import SemHashTurkishDetector
        
        # Initialize detector
        detector = SemHashTurkishDetector(config)
        
        # Process DataFrame
        start_time = time.time()
        result_df, stats = detector.process_with_statistics(df)
        processing_time = time.time() - start_time
        
        # Analyze results
        results = {
            'success': True,
            'processing_time': processing_time,
            'statistics': stats,
            'result_df_shape': result_df.shape,
            'duplicate_groups': []
        }
        
        # Extract duplicate groups if available
        if 'duplicate_group' in result_df.columns:
            duplicate_groups = result_df[result_df['duplicate_group'] >= 0].groupby('duplicate_group')
            for group_id, group in duplicate_groups:
                results['duplicate_groups'].append({
                    'group_id': group_id,
                    'size': len(group),
                    'texts': group['text'].tolist()
                })
        
        return results
        
    except ImportError as e:
        print(f"❌ SemHash Turkish detector not available: {e}")
        return {'success': False, 'error': str(e)}
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return {'success': False, 'error': str(e)}

def test_traditional_detector(df: pd.DataFrame, config: Dict[str, Any]) -> Dict[str, Any]:
    """Test traditional Turkish detector for comparison"""
    print(f"\n🔧 Testing traditional detector with config: {config}")
    
    try:
        from src.data_quality.processors.duplicate_detector import TurkishDuplicateDetector
        
        # Initialize detector
        detector = TurkishDuplicateDetector(config)
        
        # Process DataFrame
        start_time = time.time()
        result_df = detector.process(df)
        processing_time = time.time() - start_time
        
        # Analyze results
        duplicate_count = result_df['is_duplicate'].sum() if 'is_duplicate' in result_df.columns else 0
        
        results = {
            'success': True,
            'processing_time': processing_time,
            'duplicate_count': duplicate_count,
            'unique_count': len(result_df) - duplicate_count,
            'result_df_shape': result_df.shape
        }
        
        return results
        
    except ImportError as e:
        print(f"❌ Traditional Turkish detector not available: {e}")
        return {'success': False, 'error': str(e)}
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return {'success': False, 'error': str(e)}

def compare_results(semhash_results: Dict[str, Any], traditional_results: Dict[str, Any]) -> Dict[str, Any]:
    """Compare results between SemHash and traditional detector"""
    comparison = {
        'semhash_success': semhash_results.get('success', False),
        'traditional_success': traditional_results.get('success', False),
        'speed_comparison': None,
        'duplicate_comparison': None
    }
    
    if semhash_results.get('success') and traditional_results.get('success'):
        semhash_time = semhash_results['processing_time']
        traditional_time = traditional_results['processing_time']
        
        comparison['speed_comparison'] = {
            'semhash_time': semhash_time,
            'traditional_time': traditional_time,
            'speed_improvement': traditional_time / semhash_time if semhash_time > 0 else 0
        }
        
        semhash_duplicates = semhash_results['statistics'].get('duplicate_count', 0)
        traditional_duplicates = traditional_results.get('duplicate_count', 0)
        
        comparison['duplicate_comparison'] = {
            'semhash_duplicates': semhash_duplicates,
            'traditional_duplicates': traditional_duplicates,
            'difference': semhash_duplicates - traditional_duplicates
        }
    
    return comparison

def run_comprehensive_test():
    """Run comprehensive test suite"""
    print("🚀 SemHash Turkish Detector Comprehensive Test Suite")
    print("="*60)
    
    # Load test data
    df = load_test_data()
    if df is None:
        print("❌ Cannot proceed without test data")
        return False
    
    # Analyze test data
    analysis = analyze_test_data(df)
    
    # Test configurations
    test_configs = [
        {
            'name': 'SemHash Multilingual',
            'config': {
                'text_column': 'text',
                'similarity_threshold': 0.8,
                'action': 'semhash',
                'remove_stopwords': True,
                'normalize_text': True,
                'min_word_length': 2,
                'use_multilingual_model': True,
                'use_ann': True
            }
        },
        {
            'name': 'SemHash Default',
            'config': {
                'text_column': 'text',
                'similarity_threshold': 0.8,
                'action': 'semhash',
                'remove_stopwords': True,
                'normalize_text': True,
                'min_word_length': 2,
                'use_multilingual_model': False,
                'use_ann': True
            }
        },
        {
            'name': 'Traditional Detector',
            'config': {
                'text_column': 'text',
                'similarity_threshold': 0.8,
                'action': 'mark',
                'remove_stopwords': True,
                'normalize_text': True,
                'min_word_length': 2
            }
        }
    ]
    
    results = {}
    
    # Run tests
    for test_config in test_configs:
        name = test_config['name']
        config = test_config['config']
        
        print(f"\n{'='*60}")
        print(f"TESTING: {name}")
        print(f"{'='*60}")
        
        if 'SemHash' in name:
            results[name] = test_semhash_detector(df, config)
        else:
            results[name] = test_traditional_detector(df, config)
    
    # Compare results
    print(f"\n{'='*60}")
    print("COMPARISON RESULTS")
    print(f"{'='*60}")
    
    if 'SemHash Multilingual' in results and 'Traditional Detector' in results:
        comparison = compare_results(
            results['SemHash Multilingual'], 
            results['Traditional Detector']
        )
        
        if comparison['speed_comparison']:
            speed = comparison['speed_comparison']
            print(f"Speed Comparison:")
            print(f"  SemHash time: {speed['semhash_time']:.2f}s")
            print(f"  Traditional time: {speed['traditional_time']:.2f}s")
            print(f"  Speed improvement: {speed['speed_improvement']:.2f}x")
        
        if comparison['duplicate_comparison']:
            dupes = comparison['duplicate_comparison']
            print(f"Duplicate Detection Comparison:")
            print(f"  SemHash duplicates: {dupes['semhash_duplicates']}")
            print(f"  Traditional duplicates: {dupes['traditional_duplicates']}")
            print(f"  Difference: {dupes['difference']}")
    
    # Detailed results
    print(f"\n{'='*60}")
    print("DETAILED RESULTS")
    print(f"{'='*60}")
    
    for name, result in results.items():
        print(f"\n{name}:")
        if result.get('success'):
            if 'statistics' in result:
                stats = result['statistics']
                print(f"  ✅ Success - Processing time: {result['processing_time']:.2f}s")
                print(f"  Original texts: {stats.get('original_count', 'N/A')}")
                print(f"  Duplicates found: {stats.get('duplicate_count', 'N/A')}")
                print(f"  Unique texts: {stats.get('unique_count', 'N/A')}")
                print(f"  Duplicate percentage: {stats.get('duplicate_percentage', 'N/A'):.1f}%")
                print(f"  Method used: {stats.get('method_used', 'N/A')}")
                print(f"  Model used: {stats.get('model_used', 'N/A')}")
                
                # Show duplicate groups
                if result.get('duplicate_groups'):
                    print(f"  Duplicate groups: {len(result['duplicate_groups'])}")
                    for group in result['duplicate_groups'][:3]:  # Show first 3 groups
                        print(f"    Group {group['group_id']}: {group['size']} texts")
            else:
                print(f"  ✅ Success - Processing time: {result['processing_time']:.2f}s")
                print(f"  Duplicates found: {result.get('duplicate_count', 'N/A')}")
                print(f"  Unique texts: {result.get('unique_count', 'N/A')}")
        else:
            print(f"  ❌ Failed: {result.get('error', 'Unknown error')}")
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    
    successful_tests = sum(1 for result in results.values() if result.get('success'))
    total_tests = len(results)
    
    print(f"Tests completed: {successful_tests}/{total_tests}")
    print(f"Success rate: {successful_tests/total_tests*100:.1f}%")
    
    if successful_tests == total_tests:
        print("🎉 All tests passed!")
        return True
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return False

def main():
    """Main function"""
    success = run_comprehensive_test()
    return 0 if success else 1

if __name__ == "__main__":
    exit(main()) 