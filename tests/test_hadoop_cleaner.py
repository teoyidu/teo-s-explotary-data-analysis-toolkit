"""
Türkçe Hadoop Temizleyici Test Dosyası
HadoopCleanerProcessor sınıfı için kapsamlı test senaryoları
"""

import unittest
import pandas as pd
import numpy as np
from typing import Dict, Any
import sys
import os

# Ana modül yolunu ekle
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Basit import yaklaşımı
try:
    from data_quality.processors.hadoop_cleaner import HadoopCleanerProcessor
except ImportError:
    # Alternatif import yöntemi
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'data_quality', 'processors'))
    from hadoop_cleaner import HadoopCleanerProcessor


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
        
        # Beklenen sonuçlar
        expected = [
            'İstanbul Veri İşleme',
            'Başarılı işlem tamamlandı',
            'Normal Türkçe metin'
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
        
        # Türkçe karakterlerin korunduğunu kontrol et
        self.assertIn('İzmir', result['log_verisi'].iloc[0])
        self.assertIn('Öğrenci', result['log_verisi'].iloc[1])
        self.assertIn('Başarılı', result['log_verisi'].iloc[2])
        
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
        result = processor.process(df)
        
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
        
        # Türkçe karakterlerin korunduğunu kontrol et
        self.assertIn('İstanbul', result['log_verisi'].iloc[0])
        self.assertIn('Öğrenci', result['log_verisi'].iloc[1])
        self.assertIn('ğüşıöç', result['log_verisi'].iloc[1])
        self.assertIn('Başarılı', result['log_verisi'].iloc[2])

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
        
        # Çoklu satır XML'in temizlendiğini kontrol et
        for i in range(len(result)):
            self.assertNotIn('<configuration>', result['log_verisi'].iloc[i])
            self.assertNotIn('<property>', result['log_verisi'].iloc[i])
            # İçeriğin korunduğunu kontrol et
            if i == 0:
                self.assertIn('İstanbul Veri İşleme', result['log_verisi'].iloc[i])
            else:
                self.assertIn('Ankara', result['log_verisi'].iloc[i])


if __name__ == '__main__':
    # Test suite'ini çalıştır
    unittest.main(verbosity=2) 