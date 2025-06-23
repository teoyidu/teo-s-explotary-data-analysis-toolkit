#!/usr/bin/env python3
"""
A script to test the Turkish Boilerplate Cleaner and save the cleaned results.
"""

import pandas as pd
import sys
import os
import logging

# Add the src directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from data_quality.processors.boilerplate_cleaner import TurkishBoilerplateCleanerProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def run_boilerplate_cleaner_test():
    """
    Tests the boilerplate cleaner with the optimal configuration and saves the results.
    """
    print("=== TESTING TURKISH BOILERPLATE CLEANER ===")
    
    # Define input and output file paths
    input_file = 'test_data_1.parquet'
    output_file = 'test_data_1_cleaned.parquet'
    
    if not os.path.exists(input_file):
        print(f"Error: Input file not found at '{input_file}'")
        return

    # Load the test data
    df = pd.read_parquet(input_file)
    print(f"Loaded {len(df)} records from '{input_file}'")

    # Store original texts for comparison
    original_texts = df['sourcefile_content'].copy()

    # Configure the boilerplate cleaner with the most effective patterns
    config = {
        'boilerplate_columns': {
            'sourcefile_content': {
                'remove_duplicates': False,
                'remove_header_footer': False,
                'template_matching': False,
                'context_aware_cleaning': False,
                'custom_patterns': [
                    # These patterns proved most effective in testing
                    r'15/\d{2}/\d{4}\s+tarihinde\s+kesin\s+olarak,\s+oyçokluğuyla\s+karar\s+verildi\.',
                    r'\(X\)\s+KARŞI\s+OY\s*:\s*.*?(?=\n|$)',
                    r'KARAR\s+SONUCU\s*:\s*.*?(?=\n|$)',
                    r'kesin\s+olarak,\s+oyçokluğuyla\s+karar\s+verildi\.',
                    r'oyçokluğuyla\s+karar\s+verildi\.',
                    r'karar\s+verildi\.',
                ],
            }
        },
        'use_turkish_embeddings': False,
        'remove_turkish_stopwords': False,
        'normalize_turkish_text': False,
        'use_legal_patterns': True,  # Use the processor's built-in patterns as well
        'debug_mode': False # Set to True for verbose logging
    }

    # Initialize and run the processor
    processor = TurkishBoilerplateCleanerProcessor(config)
    processed_df = processor.process(df)

    print("\n=== COMPARISON: BEFORE AND AFTER CLEANING (FIRST 3 RECORDS) ===")
    for i in range(min(3, len(processed_df))):
        original_text = original_texts.iloc[i]
        cleaned_text = processed_df.iloc[i]['sourcefile_content']
        original_length = len(original_text)
        cleaned_length = len(cleaned_text)
        
        print(f"\n--- Record {i+1} ---")
        print(f"Original Length: {original_length}, Cleaned Length: {cleaned_length}")
        
        if original_length > 0:
            reduction = ((original_length - cleaned_length) / original_length * 100)
            print(f"Reduction: {reduction:.1f}%")
        
        print("\nOriginal Text (first 300 chars):")
        print(original_text[:300])
        print("\nCleaned Text (first 300 chars):")
        print(cleaned_text[:300])
        print("-" * 20)

    # Save the cleaned DataFrame to a new Parquet file
    processed_df.to_parquet(output_file, index=False)
    print(f"\nSuccessfully saved cleaned data to '{output_file}'")

if __name__ == "__main__":
    run_boilerplate_cleaner_test() 