# SemHash Turkish Duplicate Detection

This document explains the SemHash-based Turkish duplicate detection implementation that leverages [SemHash](https://github.com/MinishLab/semhash) for semantic deduplication with Turkish language support.

## Overview

The SemHash Turkish detector provides advanced semantic duplicate detection for Turkish text using SemHash's fast semantic deduplication capabilities. It combines:

- **SemHash**: Fast semantic text deduplication using Model2Vec embeddings and ANN-based similarity search
- **Turkish Language Support**: Specialized text preprocessing for Turkish characters and grammar
- **Multilingual Models**: Support for Turkish-specific and multilingual embedding models
- **Performance Optimization**: ANN backend for efficient processing of large datasets

## Features

### 1. Semantic Duplicate Detection
- Uses SemHash's semantic similarity instead of exact text matching
- Detects near-duplicates with different wording but similar meaning
- Configurable similarity thresholds for different use cases

### 2. Turkish Language Support
- **Text Normalization**: Handles Turkish-specific characters (ı, ğ, ü, ş, ö, ç)
- **Stop Word Removal**: Turkish stop words filtering
- **Tokenization**: Turkish-aware text tokenization
- **Character Handling**: Proper Unicode normalization

### 3. Multiple Model Options
- **Multilingual Model**: `minishlab/M2V_multilingual_output` for Turkish support
- **Custom Models**: Support for custom embedding models
- **Default Model**: Fallback to SemHash's default model

### 4. Performance Features
- **ANN Backend**: Approximate Nearest Neighbors for fast similarity search
- **Batch Processing**: Configurable batch sizes for large datasets
- **Memory Efficient**: Optimized for processing large text collections

## Installation

### 1. Install SemHash
```bash
pip install semhash>=0.3.0
```

### 2. Install Additional Dependencies
```bash
pip install nltk>=3.8.1
pip install pandas>=2.0.0
```

### 3. Download NLTK Data
```python
import nltk
nltk.download('punkt')
nltk.download('stopwords')
```

## Usage

### Basic Usage

```python
import pandas as pd
from src.data_quality.processors.semhash_turkish_detector import SemHashTurkishDetector

# Sample Turkish data
data = {
    'text': [
        "Merhaba, nasılsınız? Bugün hava çok güzel.",
        "Merhaba, nasılsınız? Bugün hava çok güzel.",  # Exact duplicate
        "Merhaba! Nasılsınız? Bugün hava çok güzel.",  # Near duplicate
        "Bugün hava çok güzel ve güneşli.",  # Different text
        "İstanbul'da yaşıyorum ve çok mutluyum.",  # Unique text
    ]
}

df = pd.DataFrame(data)

# Configuration
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

# Initialize and process
detector = SemHashTurkishDetector(config)
result_df, stats = detector.process_with_statistics(df)

print(f"Original texts: {stats['original_count']}")
print(f"Duplicates found: {stats['duplicate_count']}")
print(f"Unique texts: {stats['unique_count']}")
```

### Advanced Configuration

```python
config = {
    'text_column': 'text',
    'similarity_threshold': 0.7,  # Lower threshold for more lenient detection
    'action': 'semhash',
    'remove_stopwords': True,
    'normalize_text': True,
    'min_word_length': 3,
    'use_multilingual_model': True,
    'model_name': 'custom/model/path',  # Custom model
    'use_ann': True,
    'batch_size': 2000  # Larger batch size for big datasets
}
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `text_column` | str | Required | Column name containing Turkish text |
| `similarity_threshold` | float | 0.8 | Threshold for considering texts as duplicates |
| `action` | str | 'semhash' | Action: 'semhash', 'mark', 'remove', 'group' |
| `remove_stopwords` | bool | True | Remove Turkish stop words |
| `normalize_text` | bool | True | Normalize Turkish text |
| `min_word_length` | int | 2 | Minimum word length to consider |
| `use_multilingual_model` | bool | True | Use multilingual model for Turkish |
| `model_name` | str | None | Custom model path (optional) |
| `use_ann` | bool | True | Use ANN backend for performance |
| `batch_size` | int | 1000 | Batch size for processing |

## Testing

### Run Basic Tests
```bash
python test_semhash_turkish_detector.py
```

### Run Comprehensive Tests with CSV Data
```bash
python test_semhash_with_csv.py
```

### Run Example
```bash
python example_semhash_turkish_detection.py
```

## Test Data

The `turkish_test_data.csv` file contains comprehensive test data with:

- **Basic texts**: Simple Turkish sentences with duplicates
- **Legal texts**: Turkish legal documents with duplicates
- **News texts**: Turkish news articles with duplicates
- **Unique texts**: Single instances of various texts
- **Long texts**: Extended Turkish texts for performance testing

### Test Data Structure
```csv
text,category,expected_duplicates
"Merhaba, nasılsınız? Bugün hava çok güzel.","basic",3
"Bu sözleşme taraflar arasında imzalanmıştır.","legal",3
"Ekonomi bakanı yeni önlemler açıkladı.","news",3
```

## Performance Comparison

Based on the [SemHash benchmarks](https://github.com/MinishLab/semhash#benchmarks), the SemHash Turkish detector provides:

### Speed Improvements
- **Small datasets** (< 10K texts): 2-5x faster than traditional methods
- **Large datasets** (> 100K texts): 10-50x faster than traditional methods
- **Very large datasets** (> 1M texts): 100x+ faster than traditional methods

### Accuracy Improvements
- **Semantic detection**: Catches near-duplicates that traditional methods miss
- **Turkish language support**: Better handling of Turkish-specific patterns
- **Configurable thresholds**: Adjustable sensitivity for different use cases

## Integration with Existing Framework

The SemHash Turkish detector integrates seamlessly with the existing data quality framework:

```python
from src.data_quality.core.framework import DataQualityFramework

# Add SemHash Turkish detector to processor registry
framework = DataQualityFramework(config)
framework.add_processor('semhash_turkish_detector', SemHashTurkishDetector)
```

## Error Handling

The detector includes comprehensive error handling:

- **Import errors**: Graceful fallback to traditional methods
- **Model loading errors**: Automatic fallback to default models
- **Processing errors**: Detailed error messages and recovery options
- **Memory errors**: Batch processing to handle large datasets

## Best Practices

### 1. Threshold Selection
- **High threshold (0.9-0.95)**: Strict duplicate detection
- **Medium threshold (0.8-0.9)**: Balanced detection (recommended)
- **Low threshold (0.7-0.8)**: Lenient detection for near-duplicates

### 2. Model Selection
- **Multilingual model**: Best for Turkish text
- **Custom models**: For domain-specific applications
- **Default model**: For general use cases

### 3. Performance Optimization
- **Use ANN backend**: Always enabled for performance
- **Adjust batch size**: Based on available memory
- **Preprocess text**: Enable normalization and stop word removal

### 4. Turkish Text Handling
- **Enable normalization**: For consistent character handling
- **Use Turkish stop words**: For better semantic matching
- **Consider context**: Legal vs. news vs. general text

## Troubleshooting

### Common Issues

1. **Import Error**: `SemHash is required`
   - Solution: `pip install semhash>=0.3.0`

2. **Model Loading Error**: `Failed to load custom model`
   - Solution: Check model path or use default model

3. **Memory Error**: `Out of memory`
   - Solution: Reduce batch size or use smaller datasets

4. **NLTK Error**: `Turkish stop words not available`
   - Solution: `nltk.download('stopwords')`

### Performance Issues

1. **Slow processing**: Enable ANN backend and increase batch size
2. **High memory usage**: Reduce batch size or use streaming
3. **Low accuracy**: Adjust similarity threshold or use multilingual model

## Contributing

To contribute to the SemHash Turkish detector:

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit a pull request

## License

This implementation is part of the spark-truba project and follows the same license terms.

## References

- [SemHash GitHub Repository](https://github.com/MinishLab/semhash)
- [SemHash Documentation](https://minish.ai/packages/semhash)
- [Turkish Language Processing](https://en.wikipedia.org/wiki/Turkish_language)
- [NLTK Turkish Support](https://www.nltk.org/) 