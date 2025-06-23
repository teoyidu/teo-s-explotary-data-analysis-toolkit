import pandas as pd
import numpy as np
import importlib.util
import os

# Dynamically import HadoopCleanerProcessor directly from its file
module_path = os.path.join('src', 'data_quality', 'processors', 'hadoop_cleaner.py')
spec = importlib.util.spec_from_file_location('hadoop_cleaner', module_path)
if spec is None or spec.loader is None:
    raise ImportError(f"Could not load spec for {module_path}")
hadoop_cleaner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hadoop_cleaner)
HadoopCleanerProcessor = hadoop_cleaner.HadoopCleanerProcessor

# 1. Create a sample DataFrame covering all features
sample_data = {
    'log_verisi': [
        '<configuration><property><name>mapreduce.job.name</name><value>İstanbul Veri İşleme</value></property></configuration>',
        '<job-id>job_123456_789</job-id> Başarılı işlem tamamlandı',
        'Normal Türkçe metin <task-id>task_001_002_003</task-id>',
        '',
        None,
        np.nan,
        '   ',
        '<property><name>şehir</name><value>İzmir</value></property>',
        '<description>Öğrenci verileri işleniyor: ğüşıöç</description>',
        '<status>Başarılı</status> <progress>%100</progress>'
    ],
    'hadoop_log': [
        'job_123456_789 task_001_002_003 attempt_001_002_003_001 container_001_002_003_004',
        'application_123456_789 executor_001 stage_001 partition_001',
        'Normal metin job_987654_321 ile birlikte',
        '<turkish_tag>Merhaba Dünya</turkish_tag>',
        '<turkish_tag>Hoş geldiniz</turkish_tag> <property>test</property>',
        'job_987654_321 task_002_003_004 Hata oluştu',
        'job_123456_789 task_001_002_003 İşlem başarılı',
        '',
        None,
        np.nan
    ],
    'json_verisi': [
        '{"mesaj": "Merhaba İstanbul", "tarih": "2024-01-15", "gizli": "veri"}',
        '{"mesaj": "Hoş geldiniz Ankara", "tarih": "2024-01-16", "gizli": "veri"}',
        '{"mesaj": "Geçerli JSON"}',
        'Geçersiz JSON {mesaj: "test"}',
        'Normal metin',
        '',
        None,
        np.nan,
        '   ',
        '{"mesaj": "Test", "tarih": "2024-01-17", "gizli": "veri"}'
    ]
}
df = pd.DataFrame(sample_data)

# 2. Write to Parquet
parquet_path = 'test_hadoop_cleaner_input.parquet'
df.to_parquet(parquet_path, index=False)

# 3. Read back from Parquet
df_parquet = pd.read_parquet(parquet_path)
print('--- Original DataFrame from Parquet ---')
print(df_parquet)

# 4. Define config for HadoopCleanerProcessor
config = {
    'hadoop_columns': {
        'log_verisi': {
            'remove_metadata': True,
            'extract_metadata': False
        },
        'hadoop_log': {
            'remove_metadata': True,
            'extract_metadata': True,
            'metadata_fields': ['job_id', 'task_id'],
            'custom_patterns': [r'<turkish_tag>.*?</turkish_tag>']
        },
        'json_verisi': {
            'preserve_structured_data': True,
            'preserve_fields': ['mesaj', 'tarih']
        }
    }
}

# 5. Process with HadoopCleanerProcessor
processor = HadoopCleanerProcessor(config)
cleaned_df = processor.process(df_parquet)

# 6. Print results
print('\n--- Cleaned DataFrame ---')
print(cleaned_df)

# 7. Optionally, write cleaned output to Parquet
cleaned_df.to_parquet('test_hadoop_cleaner_output.parquet', index=False) 