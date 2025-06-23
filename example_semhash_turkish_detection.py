#!/usr/bin/env python3
"""
Simple example demonstrating SemHash Turkish duplicate detection
"""

import pandas as pd
import sys
import os

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    """Demonstrate SemHash Turkish duplicate detection"""
    
    print("🔍 SemHash Turkish Duplicate Detection Example")
    print("="*50)
    
    # Sample Turkish text data with various types of duplicates
    sample_data = {
        'text': [
            # Exact duplicates
            "Merhaba, nasılsınız? Bugün hava çok güzel.",
            "Merhaba, nasılsınız? Bugün hava çok güzel.",
            "Merhaba, nasılsınız? Bugün hava çok güzel.",
            
            # Near duplicates with punctuation differences
            "Merhaba! Nasılsınız? Bugün hava çok güzel.",
            "Merhaba, nasılsınız? Bugün hava çok güzel!",
            
            # Similar meaning, different wording
            "Bugün hava çok güzel ve güneşli.",
            "Hava bugün çok güzel, güneşli bir gün.",
            
            # Legal text duplicates
            "Bu sözleşme taraflar arasında imzalanmıştır.",
            "Bu sözleşme taraflar arasında imzalanmıştır.",
            "Bu sözleşme, taraflar arasında imzalanmıştır.",
            
            # News text duplicates
            "Ekonomi bakanı yeni önlemler açıkladı.",
            "Ekonomi bakanı yeni önlemler açıkladı.",
            "Ekonomi Bakanı yeni önlemler açıkladı.",
            
            # Unique texts
            "İstanbul'da yaşıyorum ve çok mutluyum.",
            "Ankara'da çalışıyorum ve memnunum.",
            "İzmir'de tatil yapıyorum.",
        ]
    }
    
    df = pd.DataFrame(sample_data)
    
    print(f"Original DataFrame: {len(df)} texts")
    print("\nSample texts:")
    for i, text in enumerate(df['text'][:5]):
        print(f"  {i}: {text}")
    
    print("\n" + "="*50)
    
    # Try to import and use SemHash Turkish detector
    try:
        from src.data_quality.processors.semhash_turkish_detector import SemHashTurkishDetector
        print("✅ SemHash Turkish detector imported successfully")
        
        # Configuration for Turkish duplicate detection
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
        
        print("\n🔧 Initializing SemHash Turkish detector...")
        detector = SemHashTurkishDetector(config)
        
        print("🔄 Processing texts for duplicate detection...")
        result_df, stats = detector.process_with_statistics(df)
        
        print("\n📊 Results:")
        print(f"  Original texts: {stats['original_count']}")
        print(f"  Duplicates found: {stats['duplicate_count']}")
        print(f"  Unique texts: {stats['unique_count']}")
        print(f"  Duplicate percentage: {stats['duplicate_percentage']:.1f}%")
        print(f"  Method used: {stats['method_used']}")
        print(f"  Model used: {stats['model_used']}")
        
        print("\n📋 Duplicate groups:")
        if 'duplicate_group' in result_df.columns:
            duplicate_groups = result_df[result_df['duplicate_group'] >= 0].groupby('duplicate_group')
            
            if len(duplicate_groups) > 0:
                for group_id, group in duplicate_groups:
                    print(f"\n  Group {group_id} ({len(group)} texts):")
                    for idx, row in group.iterrows():
                        print(f"    Row {idx}: {row['text']}")
            else:
                print("  No duplicate groups found")
        else:
            print("  No duplicate group information available")
            
        print("\n✅ SemHash Turkish duplicate detection completed successfully!")
        
    except ImportError as e:
        print(f"❌ SemHash Turkish detector not available: {e}")
        print("\nTo install SemHash, run:")
        print("  pip install semhash")
        
        # Fallback to traditional duplicate detection
        print("\n🔄 Falling back to traditional duplicate detection...")
        
        try:
            from src.data_quality.processors.duplicate_detector import TurkishDuplicateDetector
            
            config = {
                'text_column': 'text',
                'similarity_threshold': 0.8,
                'action': 'mark',
                'remove_stopwords': True,
                'normalize_text': True,
                'min_word_length': 2
            }
            
            detector = TurkishDuplicateDetector(config)
            result_df = detector.process(df)
            
            duplicate_count = result_df['is_duplicate'].sum() if 'is_duplicate' in result_df.columns else 0
            print(f"\n📊 Traditional detection results:")
            print(f"  Duplicates found: {duplicate_count}")
            print(f"  Unique texts: {len(result_df) - duplicate_count}")
            
        except ImportError as e2:
            print(f"❌ Traditional detector also not available: {e2}")
            print("\nNo duplicate detection methods available.")
    
    print("\n" + "="*50)
    print("Example completed!")

if __name__ == "__main__":
    main() 