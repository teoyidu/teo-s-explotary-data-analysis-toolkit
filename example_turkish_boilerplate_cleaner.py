#!/usr/bin/env python3
"""
Example demonstrating the enhanced Turkish boilerplate cleaner for legal data
"""

import pandas as pd
import sys
import os
import json

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def main():
    """Demonstrate Turkish boilerplate cleaner with legal data"""
    
    print("🧹 Enhanced Turkish Boilerplate Cleaner Example")
    print("="*60)
    
    # Sample Turkish legal data with boilerplate
    sample_data = {
        'text': [
            # Legal document with Turkish boilerplate
            """TÜRKİYE CUMHURİYETİ
            ANKARA 1. ASLİYE HUKUK MAHKEMESİ
            
            Sayfa 1 / 3
            
            Belge No: 2024/1234
            Tarih: 15.01.2024
            
            DAVA KONUSU: Ticari sözleşme uyuşmazlığı
            
            Bu sözleşme taraflar arasında imzalanmıştır.
            
            Madde 1: Taraflar arasında anlaşma sağlanmıştır.
            Madde 2: Bu belge ile ilgili tüm haklar saklıdır.
            
            GİRİŞ
            Yukarıda adı ve soyadı yazılı davacı tarafından açılan dava...
            
            SONUÇ
            Mahkeme kararı kesinleşmiştir.
            
            Karar bu şekilde verilmiştir.
            
            Sayfa 3 / 3""",
            
            # Another legal document with similar boilerplate
            """T.C.
            İSTANBUL 2. ASLİYE HUKUK MAHKEMESİ
            
            Sayfa 1 / 2
            
            Referans No: 2024/5678
            Tarih ve saat: 20.01.2024 14:30
            
            Dava türü: İş hukuku
            Talep eden: Ahmet Yılmaz
            Davalı: ABC Şirketi
            
            Bu sözleşme, taraflar arasında imzalanmıştır.
            
            Bölüm 1: Genel hükümler
            Fıkra 1: Taraflar arasında anlaşma sağlanmıştır.
            
            HÜKÜM
            Yargıtay kararı kesinleşmiştir.
            
            Hüküm bu şekilde verilmiştir.
            Taraflar bilgilendirilmiştir.
            
            Sayfa 2 / 2""",
            
            # Document with duplicate content
            """TÜRKİYE CUMHURİYETİ
            ANKARA 1. ASLİYE HUKUK MAHKEMESİ
            
            Sayfa 1 / 3
            
            Belge No: 2024/1234
            Tarih: 15.01.2024
            
            DAVA KONUSU: Ticari sözleşme uyuşmazlığı
            
            Bu sözleşme taraflar arasında imzalanmıştır.
            
            Madde 1: Taraflar arasında anlaşma sağlanmıştır.
            Madde 2: Bu belge ile ilgili tüm haklar saklıdır.
            
            GİRİŞ
            Yukarıda adı ve soyadı yazılı davacı tarafından açılan dava...
            
            SONUÇ
            Mahkeme kararı kesinleşmiştir.
            
            Karar bu şekilde verilmiştir.
            
            Sayfa 3 / 3""",
            
            # Clean legal content (what we want to keep)
            """Yukarıda adı ve soyadı yazılı davacı tarafından açılan dava, 
            ticari sözleşme uyuşmazlığı konusunda taraflar arasında anlaşma 
            sağlanması amacıyla açılmıştır. Davacı, sözleşme şartlarına uyulmadığını 
            iddia etmekte ve zararının tazminini talep etmektedir. Mahkeme, 
            delilleri değerlendirerek kararını vermiştir.""",
            
            # Another clean legal content
            """İş hukuku kapsamında açılan davada, işveren tarafından işçinin 
            haklarının ihlal edildiği iddia edilmektedir. İşçi, ücret alacağı 
            ve tazminat talep etmektedir. Mahkeme, iş kanunu hükümlerini 
            uygulayarak kararını vermiştir."""
        ]
    }
    
    df = pd.DataFrame(sample_data)
    
    print(f"Original DataFrame: {len(df)} documents")
    print("\nSample documents:")
    for i, text in enumerate(df['text'][:2]):
        print(f"\nDocument {i+1}:")
        print(f"{text[:200]}...")
    
    print("\n" + "="*60)
    
    # Try to import and use the enhanced Turkish boilerplate cleaner
    try:
        from src.data_quality.processors.boilerplate_cleaner import TurkishBoilerplateCleanerProcessor
        print("✅ Enhanced Turkish boilerplate cleaner imported successfully")
        
        # Configuration for Turkish legal boilerplate cleaning
        config = {
            'boilerplate_columns': {
                'text': {
                    'remove_duplicates': True,
                    'remove_header_footer': True,
                    'template_matching': True,
                    'context_aware_cleaning': True,
                    'similarity_threshold': 0.8,
                    'use_tfidf': True,
                    'custom_patterns': [
                        r'^\s*TÜRKİYE CUMHURİYETİ.*?$',
                        r'^\s*T\.C\..*?$',
                        r'^\s*Sayfa\s+\d+.*?$',
                        r'^\s*Belge\s+No:.*?$',
                        r'^\s*Referans\s+No:.*?$',
                        r'^\s*Tarih:.*?$',
                        r'^\s*Dava\s+konusu:.*?$',
                        r'^\s*Dava\s+türü:.*?$',
                        r'^\s*Talep\s+eden:.*?$',
                        r'^\s*Davalı:.*?$',
                        r'^\s*Bu\s+sözleşme.*?imzalanmıştır.*?$',
                        r'^\s*Madde\s+\d+.*?$',
                        r'^\s*Bölüm\s+\d+.*?$',
                        r'^\s*Fıkra\s+\d+.*?$',
                        r'^\s*GİRİŞ.*?$',
                        r'^\s*SONUÇ.*?$',
                        r'^\s*HÜKÜM.*?$',
                        r'^\s*Karar\s+bu\s+şekilde\s+verilmiştir.*?$',
                        r'^\s*Hüküm\s+bu\s+şekilde\s+verilmiştir.*?$',
                        r'^\s*Taraflar\s+bilgilendirilmiştir.*?$',
                        r'^\s*Mahkeme\s+kararı\s+kesinleşmiştir.*?$',
                        r'^\s*Yargıtay\s+kararı\s+kesinleşmiştir.*?$',
                    ],
                    'turkish_templates': [
                        {
                            'pattern': r'^\s*TÜRKİYE CUMHURİYETİ.*?$',
                            'replacement': ''
                        },
                        {
                            'pattern': r'^\s*T\.C\..*?$',
                            'replacement': ''
                        },
                        {
                            'pattern': r'^\s*Sayfa\s+\d+.*?$',
                            'replacement': ''
                        }
                    ],
                    'turkish_context_rules': [
                        {
                            'context_pattern': r'Mahkeme|Yargıtay|Danıştay',
                            'pattern': r'^\s*Karar\s+kesinleşmiştir.*?$',
                            'action': 'remove'
                        },
                        {
                            'context_pattern': r'sözleşme|contract',
                            'pattern': r'^\s*Bu\s+sözleşme.*?imzalanmıştır.*?$',
                            'action': 'remove'
                        }
                    ]
                }
            },
            'use_turkish_embeddings': True,
            'embedding_model': 'bge_m3',  # or 'semhash' or 'tfidf'
            'similarity_threshold': 0.8,
            'remove_turkish_stopwords': True,
            'normalize_turkish_text': True,
            'use_legal_patterns': True
        }
        
        print("\n🔧 Initializing enhanced Turkish boilerplate cleaner...")
        cleaner = TurkishBoilerplateCleanerProcessor(config)
        
        print("🔄 Processing documents for boilerplate cleaning...")
        result_df = cleaner.process(df)
        
        print("\n📊 Results:")
        print(f"  Original documents: {len(df)}")
        print(f"  Processed documents: {len(result_df)}")
        
        print("\n📋 Before and After Comparison:")
        for i in range(min(3, len(df))):
            print(f"\nDocument {i+1}:")
            print(f"  BEFORE ({len(df.iloc[i]['text'])} chars):")
            print(f"    {df.iloc[i]['text'][:150]}...")
            print(f"  AFTER ({len(result_df.iloc[i]['text'])} chars):")
            print(f"    {result_df.iloc[i]['text'][:150]}...")
            
            # Calculate reduction (avoid division by zero)
            original_len = len(df.iloc[i]['text'])
            cleaned_len = len(result_df.iloc[i]['text'])
            if original_len > 0:
                reduction = ((original_len - cleaned_len) / original_len) * 100
                print(f"  REDUCTION: {reduction:.1f}%")
            else:
                print(f"  REDUCTION: N/A (original text was empty)")
            
            # Show more detailed comparison for the first document
            if i == 0:
                print(f"\n  DETAILED COMPARISON:")
                print(f"  Original text length: {original_len}")
                print(f"  Cleaned text length: {cleaned_len}")
                print(f"  Characters removed: {original_len - cleaned_len}")
                
                # Show what was removed
                if original_len > cleaned_len:
                    print(f"  Sample of removed content:")
                    # Find what was removed by comparing line by line
                    original_lines = df.iloc[i]['text'].split('\n')
                    cleaned_lines = result_df.iloc[i]['text'].split('\n')
                    
                    removed_lines = []
                    for line in original_lines:
                        if line.strip() and line.strip() not in [cl.strip() for cl in cleaned_lines if cl.strip()]:
                            removed_lines.append(line.strip())
                    
                    for j, removed_line in enumerate(removed_lines[:5]):  # Show first 5 removed lines
                        print(f"    - {removed_line}")
                    if len(removed_lines) > 5:
                        print(f"    ... and {len(removed_lines) - 5} more lines")
        
        print("\n✅ Enhanced Turkish boilerplate cleaning completed successfully!")
        
        # Show configuration summary
        print(f"\n🔧 Configuration Summary:")
        print(f"  Embedding model: {config['embedding_model']}")
        print(f"  Turkish embeddings: {config['use_turkish_embeddings']}")
        print(f"  Legal patterns: {config['use_legal_patterns']}")
        print(f"  Turkish stopwords: {config['remove_turkish_stopwords']}")
        print(f"  Text normalization: {config['normalize_turkish_text']}")
        print(f"  Similarity threshold: {config['similarity_threshold']}")
        
    except ImportError as e:
        print(f"❌ Enhanced Turkish boilerplate cleaner not available: {e}")
        print("\nTo install required dependencies, run:")
        print("  pip install FlagEmbedding semhash nltk")
        
        # Fallback to basic cleaning
        print("\n🔄 Falling back to basic cleaning...")
        
        try:
            from src.data_quality.processors.boilerplate_cleaner import BoilerplateCleanerProcessor
            
            basic_config = {
                'boilerplate_columns': {
                    'text': {
                        'remove_duplicates': True,
                        'remove_header_footer': True,
                        'similarity_threshold': 0.8,
                        'use_tfidf': True,
                        'custom_patterns': [
                            r'^\s*TÜRKİYE CUMHURİYETİ.*?$',
                            r'^\s*T\.C\..*?$',
                            r'^\s*Sayfa\s+\d+.*?$',
                            r'^\s*Belge\s+No:.*?$',
                            r'^\s*Bu\s+sözleşme.*?imzalanmıştır.*?$',
                        ]
                    }
                }
            }
            
            cleaner = BoilerplateCleanerProcessor(basic_config)
            result_df = cleaner.process(df)
            
            print(f"\n📊 Basic cleaning results:")
            print(f"  Original documents: {len(df)}")
            print(f"  Processed documents: {len(result_df)}")
            
        except ImportError as e2:
            print(f"❌ Basic cleaner also not available: {e2}")
            print("\nNo boilerplate cleaning methods available.")
    
    print("\n" + "="*60)
    print("Example completed!")

if __name__ == "__main__":
    main() 