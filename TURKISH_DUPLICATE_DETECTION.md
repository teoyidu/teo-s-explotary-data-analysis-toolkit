# Turkish-Friendly Duplicate Detection

This document explains the Turkish language support added to the duplicate detection processor.

## Features

### 1. Turkish Text Normalization
- Handles Turkish-specific characters (ı, ğ, ü, ş, ö, ç)
- Normalizes Unicode characters
- Removes extra whitespace
- Converts text to lowercase

### 2. Turkish Stop Words
- Automatically loads Turkish stop words from NLTK
- Fallback to a comprehensive Turkish stop words list if NLTK data is not available
- Configurable stop word removal

### 3. Turkish Tokenization
- Uses NLTK's Turkish language tokenizer
- Proper sentence and word tokenization for Turkish text
- Handles Turkish punctuation and grammar rules

### 4. Configurable Parameters
- `remove_stopwords`: Enable/disable Turkish stop word removal
- `normalize_text`: Enable/disable Turkish text normalization
- `min_word_length`: Minimum word length to consider
- `similarity_threshold`: Adjustable threshold for Turkish text similarity

## Usage

### Basic Usage

```python
from src.data_quality.processors.duplicate_detector import TurkishDuplicateDetector
import pandas as pd

# Sample Turkish data
data = {
    'text': [
        "Merhaba, nasılsınız? Bugün hava çok güzel.",
        "Merhaba! Nasılsınız? Bugün hava çok güzel.",
        "İstanbul'da yaşıyorum ve çok mutluyum.",
        "İstanbul'da yaşıyorum ve çok mutluyum."
    ]
}

df = pd.DataFrame(data)

# Configuration
config = {
    'text_column': 'text',
    'similarity_threshold': 0.8,
    'action': 'mark',
    'remove_stopwords': True,
    'normalize_text': True,
    'min_word_length': 2
}

# Initialize and process
detector = TurkishDuplicateDetector(config)
result = detector.process(df)
```

### Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `text_column` | str | Required | Column name containing Turkish text |
| `similarity_threshold` | float | 0.9 | Threshold for considering texts as duplicates |
| `num_perm` | int | 128 | Number of permutations for MinHash |
| `shingle_size` | int | 3 | Size of shingles for text tokenization |
| `action` | str | 'mark' | Action: 'mark', 'remove', or 'group' |
| `remove_stopwords` | bool | True | Remove Turkish stop words |
| `normalize_text` | bool | True | Normalize Turkish text |
| `min_word_length` | int | 2 | Minimum word length to consider |

### Actions

1. **Mark Duplicates** (`action='mark'`)
   - Adds `is_duplicate` and `duplicate_group` columns
   - Preserves all original data

2. **Remove Duplicates** (`action='remove'`)
   - Removes duplicate rows, keeping the first occurrence
   - Returns cleaned DataFrame

3. **Group Duplicates** (`action='group'`)
   - Adds `duplicate_group` column
   - Groups similar texts together

## Turkish Text Processing Pipeline

1. **Text Normalization**
   - Convert to lowercase
   - Normalize Unicode characters
   - Handle Turkish-specific characters
   - Remove extra whitespace

2. **Tokenization**
   - Sentence tokenization using Turkish language model
   - Word tokenization with Turkish grammar rules
   - Remove punctuation and numbers
   - Filter by minimum word length

3. **Stop Word Removal**
   - Remove common Turkish stop words
   - Configurable stop word list

4. **Shingle Creation**
   - Create overlapping word sequences
   - Generate MinHash signatures

5. **Duplicate Detection**
   - Use Locality Sensitive Hashing (LSH)
   - Find similar text groups
   - Apply similarity threshold

## Example Output

```python
# Input DataFrame
   text
0  Merhaba, nasılsınız? Bugün hava çok güzel.
1  Merhaba! Nasılsınız? Bugün hava çok güzel.
2  İstanbul'da yaşıyorum ve çok mutluyum.
3  İstanbul'da yaşıyorum ve çok mutluyum.

# Output with mark action
   text                                              is_duplicate  duplicate_group
0  Merhaba, nasılsınız? Bugün hava çok güzel.       True         0
1  Merhaba! Nasılsınız? Bugün hava çok güzel.       True         0
2  İstanbul'da yaşıyorum ve çok mutluyum.           True         1
3  İstanbul'da yaşıyorum ve çok mutluyum.           True         1
```

## Performance Considerations

- Turkish text normalization adds minimal overhead
- Stop word removal reduces processing time
- Adjust `similarity_threshold` based on your needs:
  - Higher threshold (0.9-0.95): More strict duplicate detection
  - Lower threshold (0.7-0.8): More lenient, catches near-duplicates

## Backward Compatibility

The original `DuplicateDetector` class now inherits from `TurkishDuplicateDetector`, so existing code will automatically benefit from Turkish language support while maintaining backward compatibility.

## Dependencies

- `nltk>=3.8.1`: For Turkish tokenization and stop words
- `datasketch>=1.5.9`: For MinHash and LSH implementation
- `pandas>=2.0.0`: For DataFrame operations
- `scikit-learn>=1.2.0`: For text processing utilities

## Installation

The required dependencies are already included in `requirements.txt`. Run:

```bash
pip install -r requirements.txt
```

NLTK data will be automatically downloaded on first use. 