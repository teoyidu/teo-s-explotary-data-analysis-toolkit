#!/usr/bin/env python3
"""
Standalone script to generate synthetic Turkish legal data for testing
"""

import pandas as pd
import numpy as np
import logging
import os
import sys
from datetime import datetime, timedelta
import random
from typing import List, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def generate_synthetic_turkish_legal_data(num_records: int = 100) -> pd.DataFrame:
    """
    Generate realistic synthetic Turkish legal data for testing
    
    Args:
        num_records (int): Number of records to generate
        
    Returns:
        pd.DataFrame: Synthetic Turkish legal data
    """
    
    # Turkish legal document templates
    legal_templates = [
        # Court decision template
        """TÜRKİYE CUMHURİYETİ
{COURT_NAME} MAHKEMESİ

Dosya No: {FILE_NUMBER}
Tarih: {DATE}

KARAR

Yukarıda dosya numarası yazılı dava dosyasında, {PLAINTIFF} tarafından {DEFENDANT} aleyhine açılan {CASE_TYPE} davası hakkında;

Mahkeme, dosyadaki delilleri inceledikten sonra aşağıdaki kararı vermiştir:

{CONTENT}

Bu karar kesinleşmiştir.

Sayfa 1 / 3""",

        # Contract template
        """SÖZLEŞME

Taraflar arasında aşağıdaki sözleşme imzalanmıştır:

Madde 1: {CONTRACT_TERM_1}
Madde 2: {CONTRACT_TERM_2}
Madde 3: {CONTRACT_TERM_3}

Bu sözleşme {DATE} tarihinde yürürlüğe girmiştir.

Taraflar bilgilendirilmiştir.
İtiraz hakkı saklıdır.

Referans No: {REFERENCE_NUMBER}""",

        # Legal notice template
        """YASAL UYARI

{COMPANY_NAME} tarafından {DATE} tarihinde yayınlanmıştır.

{CONTENT}

Bu belge ile ilgili tüm haklar saklıdır.
Telif hakkı {COMPANY_NAME} tarafından korunmaktadır.

Belge No: {DOCUMENT_ID}""",

        # Administrative decision template
        """İDARİ KARAR

T.C. {MINISTRY_NAME} Bakanlığı
{AGENCY_NAME}

Karar No: {DECISION_NUMBER}
Tarih: {DATE}

GEREKÇE:
{REASONING}

KARAR:
{DECISION}

Bu karar yasal süre içinde kesinleşmiştir.

Sayfa 1 / 2""",

        # Legal opinion template
        """HUKUKİ GÖRÜŞ

{LAW_FIRM_NAME}
Avukatlık Bürosu

Tarih: {DATE}
Referans: {REFERENCE}

{CLIENT_NAME} tarafından talep edilen hukuki görüş aşağıdaki gibidir:

{OPINION}

Bu görüş sadece bilgilendirme amaçlıdır.
Yasal danışmanlık için lütfen bizimle iletişime geçiniz.

İletişim: {CONTACT_INFO}"""
    ]
    
    # Turkish legal content snippets
    legal_content_snippets = [
        "Taraflar arasında anlaşma sağlanmıştır. Davacının talepleri kabul edilmiştir.",
        "Mahkeme, delilleri inceledikten sonra davayı reddetmiştir. Gerekçe olarak yeterli delil bulunmaması gösterilmiştir.",
        "Taraflar arasında sulh anlaşması yapılmıştır. Anlaşma şartları taraflarca kabul edilmiştir.",
        "Dava konusu işlem hukuka aykırı bulunmuştur. İptal kararı verilmiştir.",
        "Taraflar arasında uzlaşma sağlanamadığından dava devam etmektedir.",
        "Mahkeme, tarafların iddialarını değerlendirdikten sonra kararını vermiştir.",
        "Yasal süre içinde itiraz edilmediğinden karar kesinleşmiştir.",
        "Taraflar arasında anlaşma sağlanmış ve dava sonlandırılmıştır.",
        "Mahkeme, delilleri yeterli bulmamış ve davayı reddetmiştir.",
        "Taraflar arasında sulh anlaşması yapılmıştır."
    ]
    
    # Turkish legal terms
    contract_terms = [
        "Taraflar arasında hizmet sözleşmesi yapılmıştır.",
        "Sözleşme süresi bir yıl olarak belirlenmiştir.",
        "Ödeme şartları taraflarca kabul edilmiştir.",
        "Sözleşme şartları değiştirilemez.",
        "Anlaşmazlık durumunda mahkeme yolu açıktır.",
        "Taraflar sözleşme şartlarına uymayı taahhüt eder.",
        "Sözleşme süresi uzatılabilir.",
        "Taraflar arasında gizlilik sözleşmesi yapılmıştır.",
        "Sözleşme şartları taraflarca kabul edilmiştir.",
        "Anlaşmazlık durumunda hakem yolu öngörülmüştür."
    ]
    
    # Turkish legal entities
    court_names = [
        "Ankara Asliye Hukuk Mahkemesi",
        "İstanbul Asliye Ticaret Mahkemesi",
        "İzmir Sulh Hukuk Mahkemesi",
        "Bursa Asliye Ceza Mahkemesi",
        "Antalya İş Mahkemesi",
        "Adana Sulh Ceza Mahkemesi",
        "Konya Asliye Hukuk Mahkemesi",
        "Gaziantep Asliye Ticaret Mahkemesi",
        "Trabzon Sulh Hukuk Mahkemesi",
        "Kayseri İş Mahkemesi"
    ]
    
    case_types = [
        "tazminat davası",
        "iş davası",
        "ticari dava",
        "aile davası",
        "miras davası",
        "gayrimenkul davası",
        "borç davası",
        "sözleşme davası",
        "haksız fiil davası",
        "maddi tazminat davası"
    ]
    
    companies = [
        "ABC Şirketi A.Ş.",
        "XYZ Limited Şirketi",
        "DEF Holding A.Ş.",
        "GHI Ticaret Ltd. Şti.",
        "JKL İnşaat A.Ş.",
        "MNO Teknoloji Ltd. Şti.",
        "PQR Enerji A.Ş.",
        "STU Sağlık Hizmetleri Ltd. Şti.",
        "VWX Eğitim Kurumları A.Ş.",
        "YZ Finans Hizmetleri Ltd. Şti."
    ]
    
    ministries = [
        "Adalet Bakanlığı",
        "İçişleri Bakanlığı",
        "Maliye Bakanlığı",
        "Sağlık Bakanlığı",
        "Eğitim Bakanlığı",
        "Ulaştırma Bakanlığı",
        "Çevre Bakanlığı",
        "Enerji Bakanlığı",
        "Tarım Bakanlığı",
        "Sanayi Bakanlığı"
    ]
    
    law_firms = [
        "Hukuk Bürosu A.Ş.",
        "Avukatlık Ortaklığı",
        "Hukuk Danışmanlığı Ltd. Şti.",
        "Avukatlık Bürosu",
        "Hukuk Hizmetleri A.Ş.",
        "Avukatlık Ortaklığı Ltd. Şti.",
        "Hukuk Danışmanlığı",
        "Avukatlık Bürosu A.Ş.",
        "Hukuk Hizmetleri Ltd. Şti.",
        "Avukatlık Ortaklığı"
    ]
    
    # Generate synthetic data
    synthetic_data = []
    
    for i in range(num_records):
        # Random template selection
        template = random.choice(legal_templates)
        
        # Generate random values
        date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime("%d.%m.%Y")
        file_number = f"{random.randint(100000, 999999)}/{random.randint(2020, 2024)}"
        reference_number = f"REF-{random.randint(10000, 99999)}"
        document_id = f"DOC-{random.randint(100000, 999999)}"
        decision_number = f"KARAR-{random.randint(1000, 9999)}"
        
        # Fill template with random values
        content = template.format(
            COURT_NAME=random.choice(court_names),
            FILE_NUMBER=file_number,
            DATE=date,
            PLAINTIFF=random.choice(companies),
            DEFENDANT=random.choice(companies),
            CASE_TYPE=random.choice(case_types),
            CONTENT=random.choice(legal_content_snippets),
            CONTRACT_TERM_1=random.choice(contract_terms),
            CONTRACT_TERM_2=random.choice(contract_terms),
            CONTRACT_TERM_3=random.choice(contract_terms),
            REFERENCE_NUMBER=reference_number,
            REFERENCE=reference_number,
            COMPANY_NAME=random.choice(companies),
            DOCUMENT_ID=document_id,
            MINISTRY_NAME=random.choice(ministries),
            AGENCY_NAME=f"{random.choice(ministries)} Genel Müdürlüğü",
            DECISION_NUMBER=decision_number,
            REASONING=random.choice(legal_content_snippets),
            DECISION=random.choice(legal_content_snippets),
            LAW_FIRM_NAME=random.choice(law_firms),
            CLIENT_NAME=random.choice(companies),
            OPINION=random.choice(legal_content_snippets),
            CONTACT_INFO=f"Tel: 0{random.randint(200, 599)} {random.randint(1000000, 9999999)}"
        )
        
        # Add some boilerplate noise
        boilerplate_noise = [
            f"Sayfa {random.randint(1, 5)} / {random.randint(5, 10)}",
            f"Belge No: {random.randint(100000, 999999)}",
            f"Tarih: {date}",
            f"Referans No: {reference_number}",
            "Bu belge ile ilgili tüm haklar saklıdır.",
            "Telif hakkı korunmaktadır.",
            "Gizli belgedir.",
            "Sadece yetkili kişiler tarafından görülebilir.",
            f"Oluşturulma tarihi: {date}",
            f"Son güncelleme: {date}"
        ]
        
        # Add random boilerplate noise
        for _ in range(random.randint(1, 3)):
            content += f"\n{random.choice(boilerplate_noise)}"
        
        # Create record
        record = {
            'DocName': f"synthetic_legal_doc_{i+1}",
            'sourcefile_content': content,
            'Category': random.choice(['Mahkeme Kararı', 'Sözleşme', 'Yasal Uyarı', 'İdari Karar', 'Hukuki Görüş']),
            'sourcefile_content_length': len(content),
            'file_name': f"synthetic_doc_{i+1}.txt",
            'estimated_tokens': len(content.split()) * 1.3,
            'language_detection': 'Turkish',
            'top_language': 'tr',
            'language_confidence': random.uniform(0.8, 1.0),
            'sourcefile_content_cleaned': content,  # Will be cleaned by processor
            'is_high_quality_turkish': True,
            'char_count': len(content),
            'word_count': len(content.split()),
            'is_char_outlier_15x': False,
            'is_char_outlier_30x': False,
            'is_word_outlier_15x': False,
            'is_word_outlier_30x': False,
            'is_any_outlier': False,
            'is_extreme_outlier': False,
            'sentence_count': len(content.split('.')),
            'avg_word_length': np.mean([len(word) for word in content.split()]),
            'avg_sentence_length': len(content.split()) / len(content.split('.')),
            'punctuation_count': sum(1 for c in content if c in '.,;:!?'),
            'punctuation_ratio': sum(1 for c in content if c in '.,;:!?') / len(content),
            'unique_words': len(set(content.split())),
            'lexical_diversity': len(set(content.split())) / len(content.split()),
            'url_count': 0,
            'email_count': 0,
            'phone_count': 1,
            'date_count': content.count('.'),
            'money_count': 0,
            'number_count': len([c for c in content if c.isdigit()]),
            'id_count': 0,
            'total_masked_elements': 0,
            'masking_density': 0.0,
            'is_high_confidence_language': True,
            'is_very_high_confidence': True,
            'confidence_category': 'High',
            'content_type': 'Legal Document',
            'source_category': 'Synthetic',
            'content_quality_score': random.uniform(0.7, 1.0),
            'quality_category': random.choice(['İyi', 'Mükemmel'])
        }
        
        synthetic_data.append(record)
    
    return pd.DataFrame(synthetic_data)

def main():
    """Main function to generate synthetic data"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Generate synthetic Turkish legal data')
    parser.add_argument('--num_records', type=int, default=100, 
                       help='Number of records to generate (default: 100)')
    parser.add_argument('--output_file', type=str, 
                       default=f'synthetic_turkish_legal_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet',
                       help='Output file name')
    parser.add_argument('--format', choices=['parquet', 'csv'], default='parquet',
                       help='Output format (default: parquet)')
    
    args = parser.parse_args()
    
    logger.info(f"Generating {args.num_records} synthetic Turkish legal records...")
    
    # Generate synthetic data
    synthetic_df = generate_synthetic_turkish_legal_data(args.num_records)
    
    # Save to file
    if args.format == 'parquet':
        synthetic_df.to_parquet(args.output_file, index=False)
    else:
        synthetic_df.to_csv(args.output_file, index=False)
    
    logger.info(f"Generated {len(synthetic_df)} records")
    logger.info(f"Saved to: {args.output_file}")
    logger.info(f"File size: {os.path.getsize(args.output_file) / 1024:.1f} KB")
    
    # Show sample
    logger.info("\n=== SAMPLE RECORDS ===")
    for i in range(min(3, len(synthetic_df))):
        logger.info(f"\n--- Record {i+1} ---")
        logger.info(f"Category: {synthetic_df.iloc[i]['Category']}")
        logger.info(f"Content length: {synthetic_df.iloc[i]['sourcefile_content_length']}")
        logger.info(f"Content preview: {synthetic_df.iloc[i]['sourcefile_content'][:200]}...")

if __name__ == "__main__":
    main() 