#!/usr/bin/env python3
"""
Türkçe Hadoop Temizleyici Standalone Test Dosyası
HadoopCleanerProcessor sınıfı için kapsamlı test senaryoları
"""

import unittest
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Set, Optional
import re
from datetime import datetime
import json
import logging
import xml.etree.ElementTree as ET

# HadoopCleanerProcessor sınıfını doğrudan burada tanımla
logger = logging.getLogger(__name__)

class HadoopCleanerProcessor:
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the processor
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.hadoop_columns = config.get('hadoop_columns', {})
        
        # Common Hadoop patterns - daha spesifik ve içeriği koruyacak şekilde
        self.hadoop_patterns = [
            r'<configuration>.*?</configuration>',  # Hadoop configuration blocks
            r'<property>.*?</property>',  # Property blocks
            r'<name>.*?</name>',  # Name tags
            r'<value>.*?</value>',  # Value tags
            r'<description>.*?</description>',  # Description tags
            r'<final>.*?</final>',  # Final tags
            r'<source>.*?</source>',  # Source tags
            r'<location>.*?</location>',  # Location tags
            r'<version>.*?</version>',  # Version tags
            r'<timestamp>.*?</timestamp>',  # Timestamp tags
            r'<job-id>.*?</job-id>',  # Job ID tags
            r'<task-id>.*?</task-id>',  # Task ID tags
            r'<attempt-id>.*?</attempt-id>',  # Attempt ID tags
            r'<status>.*?</status>',  # Status tags
            r'<progress>.*?</progress>',  # Progress tags
            r'<counters>.*?</counters>',  # Counters tags
            r'<counter-group>.*?</counter-group>',  # Counter group tags
            r'<counter>.*?</counter>',  # Counter tags
        ]
        
        # Common Hadoop metadata patterns - capture groups eklendi
        self.metadata_patterns = {
            'job_id': r'(job_\d+_\d+)',
            'task_id': r'(task_\d+_\d+_\d+)',
            'attempt_id': r'(attempt_\d+_\d+_\d+_\d+)',
            'container_id': r'(container_\d+_\d+_\d+_\d+)',
            'application_id': r'(application_\d+_\d+)',
            'executor_id': r'(executor_\d+)',
            'stage_id': r'(stage_\d+)',
            'partition_id': r'(partition_\d+)'
        }
        
        # For generic XML tag removal
        self.xml_tag_pattern = r'<[^>]+>'
        
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process the DataFrame to clean Hadoop tags
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Processed DataFrame
        """
        if not self.hadoop_columns:
            return df
        
        # Create a copy once for all metadata extraction
        original_df = df.copy(deep=True)
        
        # Extract metadata first for all columns that need it
        for column, settings in self.hadoop_columns.items():
            if column not in df.columns:
                continue
            
            if settings.get('extract_metadata', False):
                df = self._extract_metadata(df, column, settings, original_df=original_df)
        
        # Then clean tags for all columns
        for column, settings in self.hadoop_columns.items():
            if column not in df.columns:
                continue
            
            # Convert to string and clean tags
            df[column] = df[column].astype(str)
            df = self._clean_hadoop_tags(df, column, settings)
        return df
        
    def _extract_value_tags(self, text: str) -> str:
        """Extract only the values inside <value> tags. If none, return all text content."""
        try:
            # Try to parse as XML
            root = ET.fromstring(f'<root>{text}</root>')
            values = [elem.text for elem in root.iter('value') if elem.text]
            if values:
                return ' '.join(values)
            # If no <value> tags, return all text content (concatenated)
            all_text = ''.join(root.itertext()).strip()
            return all_text
        except Exception:
            # If not XML, fallback to removing tags
            return re.sub(self.xml_tag_pattern, '', text)
        
    def _clean_hadoop_tags(self, df: pd.DataFrame, column: str, settings: Dict[str, Any]) -> pd.DataFrame:
        """Clean Hadoop-specific tags from text data, preserving only <value> tag content if present."""
        def clean_text(text):
            try:
                cleaned_text = text
                # Extract only <value> tag content if present, else all text
                cleaned_text = self._extract_value_tags(cleaned_text)
                # Remove custom patterns if specified
                patterns = settings.get('custom_patterns', [])
                for pattern in patterns:
                    cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.DOTALL)
                # Remove Hadoop-specific metadata if specified
                if settings.get('remove_metadata', True):
                    for pattern in self.metadata_patterns.values():
                        cleaned_text = re.sub(pattern, '', cleaned_text)
                # Preserve structured data if specified
                if settings.get('preserve_structured_data', False):
                    try:
                        if cleaned_text.strip().startswith('{') or cleaned_text.strip().startswith('['):
                            data = json.loads(cleaned_text)
                            if 'preserve_fields' in settings:
                                data = {k: v for k, v in data.items() if k in settings['preserve_fields']}
                            cleaned_text = json.dumps(data)
                    except json.JSONDecodeError:
                        pass
                # Remove extra whitespace
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                return cleaned_text
            except Exception as e:
                logger.warning(f"Error cleaning Hadoop tags in column {column}: {str(e)}")
                return text
                
        # Apply cleaning to the column
        df[column] = df[column].apply(clean_text)
        
        return df
        
    def _extract_metadata(self, df: pd.DataFrame, column: str, settings: Dict[str, Any], original_df: pd.DataFrame) -> pd.DataFrame:
        """Extract Hadoop metadata into separate columns"""
        try:
            metadata_fields = settings.get('metadata_fields', list(self.metadata_patterns.keys()))
            for field in metadata_fields:
                if field in self.metadata_patterns:
                    pattern = self.metadata_patterns[field]
                    new_column = f"{column}_{field}"
                    df[new_column] = original_df[column].str.extract(pattern, expand=False)
                    if 'metadata_transformations' in settings and field in settings['metadata_transformations']:
                        transform = settings['metadata_transformations'][field]
                        if transform.get('type') == 'datetime':
                            df[new_column] = pd.to_datetime(df[new_column], format=transform.get('format'))
                        elif transform.get('type') == 'numeric':
                            df[new_column] = pd.to_numeric(df[new_column], errors='coerce')
            return df
        except Exception as e:
            logger.warning(f"Error extracting metadata from column {column}: {str(e)}")
            return df


class TestHadoopCleanerProcessor(unittest.TestCase):
    """HadoopCleanerProcessor için Türkçe test sınıfı"""
    
    def setUp(self):
        """Her test öncesi çalışacak kurulum"""
        self.basic_config = {
            'hadoop_columns': {
                'log_verisi': {
                    'remove_metadata': True,
                    'extract_metadata': False
                }
            }
        }
        
        self.extended_config = {
            'hadoop_columns': {
                'hadoop_log': {
                    'remove_metadata': True,
                    'extract_metadata': True,
                    'metadata_fields': ['job_id', 'task_id'],
                    'custom_patterns': [r'<turkish_tag>.*?</turkish_tag>']
                }
            }
        }
        
        self.json_config = {
            'hadoop_columns': {
                'json_verisi': {
                    'preserve_structured_data': True,
                    'preserve_fields': ['mesaj', 'tarih']
                }
            }
        }

    def test_basit_hadoop_temizleme(self):
        """Basit Hadoop etiketlerini temizleme testi"""
        processor = HadoopCleanerProcessor(self.basic_config)
        
        test_data = {
            'log_verisi': [
                '<configuration><property><name>mapreduce.job.name</name><value>İstanbul Veri İşleme</value></property></configuration>',
                '<job-id>job_123456_789</job-id> Başarılı işlem tamamlandı',
                'Normal Türkçe metin <task-id>task_001_002_003</task-id>'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Beklenen sonuçlar - sadece <value> tag içeriği
        expected = [
            'İstanbul Veri İşleme',  # Sadece <value> içeriği
            'Başarılı işlem tamamlandı',  # <job-id> tag'i kaldırıldı
            'Normal Türkçe metin'  # <task-id> tag'i kaldırıldı
        ]
        
        for i, expected_text in enumerate(expected):
            self.assertIn(expected_text, result['log_verisi'].iloc[i])
            # Hadoop etiketlerinin kaldırıldığını kontrol et
            self.assertNotIn('<configuration>', result['log_verisi'].iloc[i])
            self.assertNotIn('<job-id>', result['log_verisi'].iloc[i])
            self.assertNotIn('<task-id>', result['log_verisi'].iloc[i])

    def test_turkce_karakterler_ile_hadoop_temizleme(self):
        """Türkçe karakterler içeren Hadoop verilerini temizleme testi"""
        processor = HadoopCleanerProcessor(self.basic_config)
        
        test_data = {
            'log_verisi': [
                '<property><name>şehir</name><value>İzmir</value></property>',
                '<description>Öğrenci verileri işleniyor</description>',
                '<status>Başarılı</status> <progress>%100</progress>'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Türkçe karakterlerin korunduğunu kontrol et - sadece <value> içeriği
        self.assertIn('İzmir', result['log_verisi'].iloc[0])  # Sadece <value> içeriği
        self.assertIn('Öğrenci verileri işleniyor', result['log_verisi'].iloc[1])  # Tüm metin
        self.assertIn('Başarılı', result['log_verisi'].iloc[2])  # Tüm metin
        
        # Hadoop etiketlerinin kaldırıldığını kontrol et
        for i in range(len(result)):
            self.assertNotIn('<property>', result['log_verisi'].iloc[i])
            self.assertNotIn('<description>', result['log_verisi'].iloc[i])
            self.assertNotIn('<status>', result['log_verisi'].iloc[i])

    def test_metadata_cikarma(self):
        """Hadoop metadata çıkarma testi"""
        processor = HadoopCleanerProcessor(self.extended_config)
        
        test_data = {
            'hadoop_log': [
                'job_123456_789 task_001_002_003 attempt_001_002_003_001 container_001_002_003_004',
                'application_123456_789 executor_001 stage_001 partition_001',
                'Normal metin job_987654_321 ile birlikte'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Metadata'nın kaldırıldığını kontrol et
        for i in range(len(result)):
            self.assertNotIn('job_', result['hadoop_log'].iloc[i])
            self.assertNotIn('task_', result['hadoop_log'].iloc[i])
            self.assertNotIn('attempt_', result['hadoop_log'].iloc[i])
            self.assertNotIn('container_', result['hadoop_log'].iloc[i])

    def test_metadata_cikarma_ve_ayirma(self):
        """Metadata çıkarma ve ayrı sütunlara ayırma testi"""
        processor = HadoopCleanerProcessor(self.extended_config)
        
        test_data = {
            'hadoop_log': [
                'job_123456_789 task_001_002_003 İşlem başarılı',
                'job_987654_321 task_002_003_004 Hata oluştu'
            ]
        }
        
        df = pd.DataFrame(test_data)
        print(f"Original data: {df['hadoop_log'].iloc[0]}")
        result = processor.process(df)
        print(f"Processed data: {result['hadoop_log'].iloc[0]}")
        print(f"Job ID column: {result['hadoop_log_job_id'].iloc[0]}")
        print(f"Task ID column: {result['hadoop_log_task_id'].iloc[0]}")
        
        # Yeni metadata sütunlarının oluşturulduğunu kontrol et
        self.assertIn('hadoop_log_job_id', result.columns)
        self.assertIn('hadoop_log_task_id', result.columns)
        
        # Metadata değerlerinin doğru çıkarıldığını kontrol et
        self.assertEqual(result['hadoop_log_job_id'].iloc[0], 'job_123456_789')
        self.assertEqual(result['hadoop_log_job_id'].iloc[1], 'job_987654_321')
        self.assertEqual(result['hadoop_log_task_id'].iloc[0], 'task_001_002_003')
        self.assertEqual(result['hadoop_log_task_id'].iloc[1], 'task_002_003_004')

    def test_ozel_pattern_temizleme(self):
        """Özel pattern temizleme testi"""
        processor = HadoopCleanerProcessor(self.extended_config)
        
        test_data = {
            'hadoop_log': [
                '<turkish_tag>Merhaba Dünya</turkish_tag>',
                '<turkish_tag>Hoş geldiniz</turkish_tag> <property>test</property>'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Özel pattern'ın kaldırıldığını kontrol et
        for i in range(len(result)):
            self.assertNotIn('<turkish_tag>', result['hadoop_log'].iloc[i])
            self.assertNotIn('</turkish_tag>', result['hadoop_log'].iloc[i])
            # İçeriğin korunduğunu kontrol et
            if i == 0:
                self.assertIn('Merhaba Dünya', result['hadoop_log'].iloc[i])
            else:
                self.assertIn('Hoş geldiniz', result['hadoop_log'].iloc[i])

    def test_json_veri_koruma(self):
        """JSON veri koruma testi"""
        processor = HadoopCleanerProcessor(self.json_config)
        
        test_data = {
            'json_verisi': [
                '{"mesaj": "Merhaba İstanbul", "tarih": "2024-01-15", "gizli": "veri"}',
                '{"mesaj": "Hoş geldiniz Ankara", "tarih": "2024-01-16", "gizli": "veri"}'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Belirtilen alanların korunduğunu kontrol et
        for i in range(len(result)):
            json_str = result['json_verisi'].iloc[i]
            self.assertIn('"mesaj"', json_str)
            self.assertIn('"tarih"', json_str)
            # Gizli alanın kaldırıldığını kontrol et
            self.assertNotIn('"gizli"', json_str)

    def test_bos_veri_isleme(self):
        """Boş veri işleme testi"""
        processor = HadoopCleanerProcessor(self.basic_config)
        
        test_data = {
            'log_verisi': ['', None, np.nan, '   ', '<property>test</property>']
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Boş verilerin işlendiğini kontrol et
        self.assertEqual(len(result), len(test_data['log_verisi']))
        # Son elemanın temizlendiğini kontrol et
        self.assertNotIn('<property>', result['log_verisi'].iloc[-1])

    def test_gecersiz_json_isleme(self):
        """Geçersiz JSON işleme testi"""
        processor = HadoopCleanerProcessor(self.json_config)
        
        test_data = {
            'json_verisi': [
                '{"mesaj": "Geçerli JSON"}',
                'Geçersiz JSON {mesaj: "test"}',
                'Normal metin'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Geçersiz JSON'ın olduğu gibi bırakıldığını kontrol et
        self.assertIn('Geçersiz JSON', result['json_verisi'].iloc[1])
        self.assertIn('Normal metin', result['json_verisi'].iloc[2])

    def test_olmayan_sutun_isleme(self):
        """Olmayan sütun işleme testi"""
        processor = HadoopCleanerProcessor(self.basic_config)
        
        test_data = {
            'baska_sutun': ['test verisi']
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Olmayan sütunun etkilenmediğini kontrol et
        self.assertEqual(result['baska_sutun'].iloc[0], 'test verisi')

    def test_bos_konfigurasyon(self):
        """Boş konfigürasyon testi"""
        empty_config = {'hadoop_columns': {}}
        processor = HadoopCleanerProcessor(empty_config)
        
        test_data = {
            'log_verisi': ['<property>test</property>']
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Hiçbir değişiklik olmadığını kontrol et
        self.assertEqual(result['log_verisi'].iloc[0], '<property>test</property>')

    def test_turkce_unicode_karakterler(self):
        """Türkçe Unicode karakterler testi"""
        processor = HadoopCleanerProcessor(self.basic_config)
        
        test_data = {
            'log_verisi': [
                '<property><name>şehir</name><value>İstanbul</value></property>',
                '<description>Öğrenci verileri işleniyor: ğüşıöç</description>',
                '<status>Başarılı</status> <progress>%100</progress>'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Türkçe karakterlerin korunduğunu kontrol et - sadece <value> içeriği
        self.assertIn('İstanbul', result['log_verisi'].iloc[0])  # Sadece <value> içeriği
        self.assertIn('Öğrenci', result['log_verisi'].iloc[1])  # Tüm metin
        self.assertIn('ğüşıöç', result['log_verisi'].iloc[1])  # Tüm metin
        self.assertIn('Başarılı', result['log_verisi'].iloc[2])  # Tüm metin

    def test_metadata_donusumleri(self):
        """Metadata dönüşümleri testi"""
        config_with_transforms = {
            'hadoop_columns': {
                'hadoop_log': {
                    'extract_metadata': True,
                    'metadata_fields': ['job_id'],
                    'metadata_transformations': {
                        'job_id': {'type': 'numeric'}
                    }
                }
            }
        }
        
        processor = HadoopCleanerProcessor(config_with_transforms)
        
        test_data = {
            'hadoop_log': [
                'job_123456_789 İşlem tamamlandı',
                'job_987654_321 Hata oluştu'
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Metadata sütununun oluşturulduğunu kontrol et
        self.assertIn('hadoop_log_job_id', result.columns)

    def test_coklu_satir_xml_temizleme(self):
        """Çoklu satır XML temizleme testi"""
        processor = HadoopCleanerProcessor(self.basic_config)
        
        test_data = {
            'log_verisi': [
                '''<configuration>
                    <property>
                        <name>mapreduce.job.name</name>
                        <value>İstanbul Veri İşleme</value>
                    </property>
                </configuration>''',
                '''<property>
                    <name>şehir</name>
                    <value>Ankara</value>
                </property>'''
            ]
        }
        
        df = pd.DataFrame(test_data)
        result = processor.process(df)
        
        # Çoklu satır XML'in temizlendiğini kontrol et - sadece <value> içeriği
        for i in range(len(result)):
            self.assertNotIn('<configuration>', result['log_verisi'].iloc[i])
            self.assertNotIn('<property>', result['log_verisi'].iloc[i])
            # İçeriğin korunduğunu kontrol et - sadece <value> içeriği
            if i == 0:
                self.assertIn('İstanbul Veri İşleme', result['log_verisi'].iloc[i])
            else:
                self.assertIn('Ankara', result['log_verisi'].iloc[i])

    def test_debug_metadata_extraction(self):
        """Debug metadata extraction issue"""
        config = {
            'hadoop_columns': {
                'test_column': {
                    'extract_metadata': True,
                    'metadata_fields': ['job_id']
                }
            }
        }
        
        processor = HadoopCleanerProcessor(config)
        
        test_data = {
            'test_column': ['job_123456_789 test data']
        }
        
        df = pd.DataFrame(test_data)
        print(f"Before processing: {df['test_column'].iloc[0]}")
        
        result = processor.process(df)
        print(f"After processing: {result['test_column'].iloc[0]}")
        print(f"Job ID extracted: {result['test_column_job_id'].iloc[0]}")
        
        # This should pass if metadata extraction works
        self.assertEqual(result['test_column_job_id'].iloc[0], 'job_123456_789')

    def test_isolated_debug(self):
        """Completely isolated debug test"""
        import pandas as pd
        
        # Create test data
        test_data = {'test_column': ['job_123456_789 test data']}
        df = pd.DataFrame(test_data)
        
        print(f"1. Original DataFrame: {df['test_column'].iloc[0]}")
        
        # Create a copy
        df_copy = df.copy(deep=True)
        print(f"2. DataFrame copy: {df_copy['test_column'].iloc[0]}")
        
        # Test regex extraction directly
        import re
        pattern = r'(job_\d+_\d+)'
        text = df_copy['test_column'].iloc[0]
        print(f"3. Text for regex: {text}")
        match = re.search(pattern, text)
        print(f"4. Regex match: {match.group(1) if match else None}")
        
        # Test pandas str.extract
        extracted = df_copy['test_column'].str.extract(pattern, expand=False)
        print(f"5. Pandas extract: {extracted.iloc[0]}")
        
        # This should pass
        self.assertEqual(extracted.iloc[0], 'job_123456_789')

    def test_fresh_processor(self):
        """Test with a completely fresh processor instance"""
        config = {
            'hadoop_columns': {
                'test_column': {
                    'extract_metadata': True,
                    'metadata_fields': ['job_id']
                }
            }
        }
        
        # Create fresh processor
        processor = HadoopCleanerProcessor(config)
        
        # Create fresh test data
        test_data = {
            'test_column': ['job_123456_789 test data']
        }
        
        df = pd.DataFrame(test_data)
        print(f"Fresh test - Before processing: {df['test_column'].iloc[0]}")
        
        result = processor.process(df)
        print(f"Fresh test - After processing: {result['test_column'].iloc[0]}")
        print(f"Fresh test - Job ID extracted: {result['test_column_job_id'].iloc[0]}")
        
        # This should pass
        self.assertEqual(result['test_column_job_id'].iloc[0], 'job_123456_789')

    def test_completely_isolated(self):
        """Completely isolated test with fresh everything"""
        import pandas as pd
        
        # Create fresh test data
        test_data = {'test_column': ['job_123456_789 test data']}
        df = pd.DataFrame(test_data)
        
        print(f"1. Fresh DataFrame: {df['test_column'].iloc[0]}")
        
        # Create fresh config
        config = {
            'hadoop_columns': {
                'test_column': {
                    'extract_metadata': True,
                    'metadata_fields': ['job_id']
                }
            }
        }
        
        # Create fresh processor
        processor = HadoopCleanerProcessor(config)
        
        # Process
        result = processor.process(df)
        
        print(f"2. After processing: {result['test_column'].iloc[0]}")
        print(f"3. Job ID extracted: {result['test_column_job_id'].iloc[0]}")
        
        # This should pass
        self.assertEqual(result['test_column_job_id'].iloc[0], 'job_123456_789')


if __name__ == '__main__':
    print("🚀 Hadoop Cleaner Testleri Başlatılıyor...")
    print("=" * 50)
    
    # Test suite'ini çalıştır
    unittest.main(verbosity=2) 