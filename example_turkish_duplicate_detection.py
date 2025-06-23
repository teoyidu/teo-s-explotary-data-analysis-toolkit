#!/usr/bin/env python3
"""
Example usage of Turkish-friendly duplicate detector
"""

import pandas as pd
from src.data_quality.processors.duplicate_detector import TurkishDuplicateDetector

def main():
    # Sample Turkish text data
    sample_data = {
        'text': [
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
    }
    
    df = pd.DataFrame(sample_data)
    
    print("Original DataFrame:")
    print(df)
    print("\n" + "="*50 + "\n")
    
    # Configuration for Turkish duplicate detection
    config = {
        'text_column': 'text',
        'similarity_threshold': 0.8,  # Lower threshold for Turkish text
        'num_perm': 128,
        'shingle_size': 3,
        'action': 'mark',  # Mark duplicates
        'remove_stopwords': True,
        'normalize_text': True,
        'min_word_length': 2
    }
    
    # Initialize Turkish duplicate detector
    detector = TurkishDuplicateDetector(config)
    
    # Process the DataFrame
    result_df = detector.process(df)
    
    print("Processed DataFrame with duplicate detection:")
    print(result_df)
    print("\n" + "="*50 + "\n")
    
    # Show duplicate groups
    duplicate_groups = result_df[result_df['duplicate_group'] >= 0].groupby('duplicate_group')
    
    print("Duplicate Groups Found:")
    for group_id, group in duplicate_groups:
        print(f"\nGroup {group_id}:")
        for idx, row in group.iterrows():
            print(f"  Row {idx}: {row['text']}")
    
    print("\n" + "="*50 + "\n")
    
    # Example with removal action
    print("Example with removal action:")
    config_remove = config.copy()
    config_remove['action'] = 'remove'
    
    detector_remove = TurkishDuplicateDetector(config_remove)
    result_remove = detector_remove.process(df)
    
    print(f"Original rows: {len(df)}")
    print(f"After removing duplicates: {len(result_remove)}")
    print(f"Removed {len(df) - len(result_remove)} duplicate rows")
    
    print("\nRemaining unique texts:")
    for idx, row in result_remove.iterrows():
        print(f"  {row['text']}")

if __name__ == "__main__":
    main() 