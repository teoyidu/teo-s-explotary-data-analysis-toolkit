# Teo's EDA Emporium

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/downloads/)
[![Spark Version](https://img.shields.io/badge/spark-3.4%2B-orange)](https://spark.apache.org/downloads.html)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-latest-brightgreen)](docs/)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()

A comprehensive, production-ready data quality framework for processing Parquet files using Apache Spark. This framework implements advanced data quality checks with batch processing capabilities, error handling, and configurable checkpointing.

## Table of Contents
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Advanced Usage](#advanced-usage)
- [Performance Tuning](#performance-tuning)
- [Error Handling](#error-handling)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)
- [Roadmap](#roadmap)

## Features

### 🔍 Advanced Data Quality Processors

The framework implements sophisticated data quality processors:

1. **BoilerplateCleanerProcessor**
   - TF-IDF based duplicate detection with configurable thresholds
   - Context-aware cleaning methods for different document types
   - Smart header/footer removal using pattern recognition
   - Template matching capabilities with customizable templates
   - Configurable similarity thresholds (default: 0.8)
   - Support for multiple languages and character encodings
   - Memory-efficient processing for large documents

2. **HadoopCleanerProcessor**
   - Metadata extraction capabilities from Hadoop ecosystem
   - Improved tag cleaning using predefined patterns
   - Structured data preservation with schema validation
   - Custom pattern support for specialized use cases
   - Metadata transformation options with field mapping
   - Support for various Hadoop file formats
   - Integration with Hadoop security features

3. **HTMLCleanerProcessor**
   - Custom tag replacements with whitelist/blacklist
   - Attribute preservation for critical HTML elements
   - Enhanced entity handling with custom entity maps
   - Whitelist-based tag filtering with configurable rules
   - Custom transformation support for specific HTML structures
   - Support for HTML5 and legacy HTML
   - XSS protection and sanitization

4. **Core Data Quality Processors**
   - **MissingValuesProcessor**
     - Advanced null/missing value detection
     - Multiple imputation strategies
     - Statistical analysis of missing patterns
     - Custom handling for different data types
     - Threshold-based alerts and reporting
   
   - **MandatoryFieldsProcessor**
     - Required field validation with custom rules
     - Cross-field dependency checking
     - Conditional mandatory field support
     - Detailed validation reporting
     - Custom error messages and handling
   
   - **NumericalFormatsProcessor**
     - Data type standardization
     - Format validation and conversion
     - Range checking and outlier detection
     - Unit conversion support
     - Precision and scale management
   
   - **OutdatedDataProcessor**
     - Temporal data filtering
     - Age-based validation
     - Timezone handling
     - Custom date/time formats
     - Relative time calculations
   
   - **ExternalValidationProcessor**
     - Cross-reference validation
     - External API integration
     - Cache management
     - Rate limiting
     - Error handling and retries
   
   - **UniquenessProcessor**
     - Duplicate detection and removal
     - Fuzzy matching support
     - Composite key validation
     - Performance optimization
     - Custom uniqueness rules
   
   - **CategoriesProcessor**
     - Categorical data standardization
     - Value mapping and normalization
     - Hierarchical category support
     - Custom category rules
     - Validation reporting
   
   - **TextValidationProcessor**
     - Pattern-based text validation
     - Regular expression support
     - Language detection
     - Character encoding handling
     - Custom validation rules
   
   - **RelationshipsProcessor**
     - Data relationship validation
     - Foreign key checking
     - Referential integrity
     - Custom relationship rules
     - Performance optimization
   
   - **EntryRulesProcessor**
     - Business rule enforcement
     - Custom rule engine
     - Rule dependency management
     - Validation reporting
     - Error handling

### 📊 Metrics & Monitoring

The framework includes comprehensive metrics collection:

1. **Performance Metrics**
   - Detailed operation timing and statistics
     - Per-operation timing
     - Cumulative timing
     - Statistical analysis
   - Memory usage tracking
     - RSS (Resident Set Size)
     - VMS (Virtual Memory Size)
     - Shared memory
     - Heap usage
   - CPU utilization monitoring
     - Per-core usage
     - Overall utilization
     - Peak usage tracking
   - I/O operation counters
     - Read/write operations
     - Network I/O
     - Disk I/O
   - Garbage collection statistics
     - Collection frequency
     - Memory reclaimed
     - Pause times
   - Operation success/failure rates
     - Per-operation tracking
     - Trend analysis
     - Alert thresholds
   - Historical metrics tracking
     - Time-series data
     - Trend analysis
     - Performance baselines

2. **Resource Monitoring**
   - Real-time memory usage tracking
     - Heap monitoring
     - Non-heap monitoring
     - GC monitoring
   - CPU utilization monitoring
     - Per-core tracking
     - Process-level monitoring
     - System-level monitoring
   - I/O operations tracking
     - Disk I/O
     - Network I/O
     - File operations
   - Thread count monitoring
     - Active threads
     - Blocked threads
     - Thread pool stats
   - Open file handles tracking
     - File descriptor limits
     - Handle leaks detection
     - Resource cleanup
   - Network connection monitoring
     - Active connections
     - Connection pools
     - Network errors
   - System resource peak tracking
     - Historical peaks
     - Resource trends
     - Alert thresholds

3. **Validation Metrics**
   - Data quality scores
     - Per-field scores
     - Overall quality score
     - Trend analysis
   - Error rates
     - Per-operation errors
     - Error categorization
     - Error trends
   - Processing success rates
     - Batch success rates
     - Record success rates
     - Error recovery rates
   - Batch statistics
     - Batch size metrics
     - Processing time
     - Resource usage
   - Operation duration statistics
     - Min/max/avg durations
     - Percentile analysis
     - Performance trends
   - Memory usage patterns
     - Usage trends
     - Peak patterns
     - Leak detection
   - CPU usage patterns
     - Usage trends
     - Bottleneck detection
     - Optimization opportunities

4. **Advanced Logging**
   - Structured JSON logging
     - Standardized format
     - Custom fields
     - Log levels
   - Detailed error context
     - Stack traces
     - Error codes
     - Context data
   - System state snapshots
     - Resource usage
     - Configuration state
     - Operation state
   - Performance summaries
     - Operation metrics
     - Resource usage
     - Error statistics
   - Batch processing metrics
     - Batch statistics
     - Processing times
     - Success rates
   - Resource utilization history
     - Historical data
     - Trend analysis
     - Capacity planning
   - Operation success/failure tracking
     - Success rates
     - Error patterns
     - Recovery statistics

### 🚀 Performance & Scalability

- **Batch Processing**
  - Configurable batch sizes
  - Dynamic batch optimization
  - Memory-aware batching
  - Progress tracking
  - Checkpoint management

- **Concurrent Processing**
  - Parallel batch execution
  - Resource-aware scheduling
  - Load balancing
  - Thread pool management
  - Task prioritization

- **Memory Optimization**
  - Intelligent caching
  - Memory management
  - Garbage collection tuning
  - Resource cleanup
  - Memory leak prevention

- **Adaptive Query Execution**
  - Spark optimization
  - Query planning
  - Execution optimization
  - Resource allocation
  - Performance tuning

- **Checkpointing**
  - Resume from failures
  - State management
  - Recovery points
  - Progress tracking
  - Data consistency

### ⚙️ Configuration & Management

- **Flexible Configuration**
  - JSON/YAML support
  - Environment variables
  - Command-line options
  - Default values
  - Validation rules

- **Environment-Specific Configs**
  - Development
  - Testing
  - Production
  - Staging
  - Custom environments

- **Domain-Specific Templates**
  - E-commerce
  - Finance
  - Healthcare
  - Manufacturing
  - Custom domains

- **Validation**
  - Schema validation
  - Value validation
  - Dependency checking
  - Error reporting
  - Auto-correction

### 🔧 Enterprise Features

- **Comprehensive Logging**
  - Multiple log levels
  - Structured format
  - Log rotation
  - Log aggregation
  - Log analysis

- **Error Handling**
  - Detailed reporting
  - Error categorization
  - Recovery strategies
  - Alert mechanisms
  - Error tracking

- **Monitoring**
  - Real-time metrics
  - Performance tracking
  - Resource monitoring
  - Alert thresholds
  - Dashboard integration

- **Recovery**
  - Checkpoint-based recovery
  - State management
  - Data consistency
  - Error recovery
  - Progress tracking

## Project Structure

```
├── src/
│   ├── data_quality/
│   │   ├── processors/         # Data quality processors
│   │   ├── core/              # Core framework components
│   │   ├── utils/             # Utility functions
│   │   ├── config/            # Configuration templates
│   │   └── tests/             # Test suite
│   ├── legal_domain_filter.py # Legal domain classification
│   ├── logging_manager.py     # Logging configuration
│   ├── performance_monitor.py # Performance monitoring
│   └── logging_config.py      # Logging settings
├── main.py                    # Main entry point
├── data_quality_framework.py  # Core framework implementation
├── batch_processor.py         # Batch processing logic
├── config_manager.py          # Configuration management
├── cli_processor.py           # Command-line interface
├── example_usage.py           # Usage examples
├── setup.py                   # Package setup
├── pyproject.toml            # Project metadata
├── requirements.txt          # Dependencies
└── install_dev.sh            # Development setup script
```

## Installation

### Prerequisites

- Python 3.8+
- Apache Spark 3.4+
- Java 8 or 11
- 8GB+ RAM recommended
- 4+ CPU cores recommended
- 20GB+ free disk space

### Quick Installation

```bash
# Clone the repository
git clone <repository-url>
cd spark-truba

# Install development dependencies
./install_dev.sh
```

### Manual Installation

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### For Distributed Deployment

```bash
# Install on all cluster nodes
pip install pyspark pandas pyarrow PyYAML scikit-learn beautifulsoup4 psutil \
    fastparquet python-snappy brotli lz4 zstandard matplotlib seaborn plotly

# Configure Spark environment
export SPARK_HOME=/path/to/spark
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PATH=$SPARK_HOME/bin:$PATH
```

## Quick Start

### 1. Create Sample Configuration Files

```bash
python main.py --create-sample-configs
```

This creates configuration files for different domains:
- `ecommerce_data_quality_config.json`
- `financial_data_quality_config.json`
- `healthcare_data_quality_config.json`

### 2. Basic Usage

```bash
# Process single file with default configuration
python main.py /path/to/your/data.parquet

# Process multiple files with custom configuration
python main.py /path/to/data1.parquet /path/to/data2.parquet \
  --config financial_data_quality_config.json \
  --output-dir /path/to/output

# Analyze data without processing
python main.py /path/to/data.parquet --analyze-only
```

### 3. Batch Processing

```bash
# Enable batch processing for large datasets
python main.py /path/to/large_dataset.parquet \
  --batch-processing \
  --config ecommerce_data_quality_config.json \
  --output-dir /data/cleaned

# Disable batch processing for small datasets
python main.py /path/to/small_dataset.parquet \
  --no-batch-processing
```

## Configuration

### Sample Configuration Structure

```json
{
  "checkpoint_dir": "/data/checkpoints",
  "output_dir": "/data/cleaned",
  "batch_size": 1000000,
  
  "processors": {
    "boilerplate_cleaner": {
      "use_tfidf": true,
      "similarity_threshold": 0.8,
      "template_matching": true,
      "context_aware_cleaning": true
    },
    "hadoop_cleaner": {
      "extract_metadata": true,
      "preserve_structured_data": true,
      "metadata_fields": ["job_id", "task_id"]
    },
    "html_cleaner": {
      "whitelist_tags": ["p", "br", "strong"],
      "preserve_attributes": true,
      "custom_entities": {
        "&nbsp;": " ",
        "&amp;": "&"
      }
    },
    "missing_values": {
      "strategy": "fill",
      "threshold": 30.0,
      "critical_columns": ["id", "timestamp"],
      "fill_values": {
        "numeric": 0,
        "string": "N/A",
        "date": "1970-01-01"
      }
    },
    "mandatory_fields": {
      "required_columns": ["id", "name", "created_at"],
      "error_action": "drop"
    },
    "numerical_formats": {
      "decimal_places": 2,
      "rounding_mode": "half_up",
      "handle_currency": true
    }
  },
  
  "metrics": {
    "enabled": true,
    "collect_memory_usage": true,
    "collect_processing_times": true,
    "collect_validation_stats": true,
    "output_format": "json",
    "output_path": "/data/metrics"
  },
  
  "spark_config": {
    "adaptive_query_execution": true,
    "vectorized_parquet_reading": true,
    "arrow_optimization": true,
    "memory_fraction": 0.8,
    "storage_fraction": 0.3,
    "shuffle_partitions": 200
  }
}
```

## Advanced Usage

### Custom Processing Function

```python
from data_quality_framework import DataQualityFramework
from config_manager import ConfigurationManager
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Custom DQ").getOrCreate()

# Load configuration
config_manager = ConfigurationManager('my_config.json')
config_dict = config_manager.to_dict()

# Initialize framework
dq_framework = DataQualityFramework(spark, config_dict)

# Process files
results = dq_framework.process_parquet_files([
    '/path/to/data1.parquet',
    '/path/to/data2.parquet'
])

# Access metrics
metrics = dq_framework.metrics.get_summary()
print(f"Processing time: {metrics['total_processing_time']} seconds")
print(f"Peak memory usage: {metrics['peak_memory_usage']} bytes")
print(f"Total records processed: {metrics['total_records_processed']}")

spark.stop()
```

### Batch Processing Integration

```python
from batch_processor import BatchProcessor, BatchOptimizer

# Create batch processor
batch_config = {
    'batch_size': 500000,
    'max_concurrent_batches': 3,
    'enable_checkpointing': True
}

processor = BatchProcessor(spark, batch_config)

# Analyze dataset for optimization
optimizer = BatchOptimizer(spark)
analysis = optimizer.analyze_dataset(df)
print(f"Recommended batch size: {analysis['recommendations']['batch_size']}")

# Process with batch processor
def my_processing_function(batch_df):
    # Your custom processing logic here
    cleaned_df = batch_df.filter(col("amount") > 0)
    stats = {'processed_rows': cleaned_df.count()}
    return cleaned_df, stats

results = processor.process_large_dataset(
    df=df,
    processing_function=my_processing_function,
    output_path="/path/to/output"
)
```

## Performance Tuning

### Spark Configuration

The framework automatically sets optimal Spark configurations and provides detailed performance monitoring:

```python
# Performance monitoring setup
logging_manager = LoggingManager(
    log_dir="logs",
    metrics_history_size=1000  # Store last 1000 metrics
)

# Get performance summary
performance_summary = logging_manager.get_performance_summary()
print(f"Operation stats: {performance_summary['operation_stats']}")
print(f"System stats: {performance_summary['system_stats']}")

# Monitor specific operation
logging_manager.start_operation("data_processing")
# ... your processing code ...
logging_manager.end_operation(
    "data_processing",
    success=True,
    additional_metrics={"records_processed": 1000}
)
```

The framework automatically sets optimal Spark configurations:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.memory.fraction", "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")
spark.conf.set("spark.shuffle.file.buffer", "1m")
spark.conf.set("spark.file.transferTo", "true")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.shuffle.compress", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

### Batch Size Recommendations

| Dataset Size | Recommended Batch Size | Concurrent Batches | Memory Usage | Processing Time |
|-------------|----------------------|-------------------|--------------|----------------|
| < 100K rows | Process all at once | 1 | Low | Fast |
| 100K - 1M rows | 100K | 2 | Medium | Medium |
| 1M - 10M rows | 500K | 3 | High | Slow |
| > 10M rows | 1M | 4 | Very High | Very Slow |

## Error Handling

### Types of Errors Handled

1. **File I/O Errors**
   - Missing files
   - Permission issues
   - Disk space errors
   - Network errors
   - File corruption

2. **Schema Validation Errors**
   - Unexpected data types
   - Missing columns
   - Invalid formats
   - Constraint violations
   - Schema evolution

3. **Data Quality Errors**
   - Constraint violations
   - Invalid data
   - Format errors
   - Range violations
   - Business rule violations

4. **Processing Errors**
   - Memory issues
   - Timeout errors
   - Resource exhaustion
   - Deadlocks
   - Race conditions

5. **Configuration Errors**
   - Invalid settings
   - Missing parameters
   - Type mismatches
   - Dependency errors
   - Environment issues

### Error Recovery

- Automatic retry for transient errors
  - Configurable retry count
  - Exponential backoff
  - Error classification
  - Success criteria
  - Failure handling

- Checkpoint-based recovery
  - State management
  - Progress tracking
  - Data consistency
  - Recovery points
  - Validation

- Detailed error logging
  - Error context
  - Stack traces
  - System state
  - User context
  - Recovery attempts

- Graceful degradation
  - Fallback options
  - Partial results
  - Error reporting
  - User notification
  - Recovery options

## Contributing

### Setting Up Development Environment

```bash
# Clone repository
git clone <repository-url>
cd spark-truba

# Install development dependencies
./install_dev.sh

# Run tests
python -m pytest tests/

# Run with sample data
python main.py --create-sample-configs
python main.py sample_data.parquet --config ecommerce_data_quality_config.json
```

### Code Style

- Follow PEP 8 guidelines
  - Line length limits
  - Naming conventions
  - Import ordering
  - Documentation
  - Whitespace

- Use type hints
  - Function parameters
  - Return values
  - Class attributes
  - Generic types
  - Optional types

- Include docstrings
  - Function purpose
  - Parameters
  - Return values
  - Examples
  - Exceptions

- Add logging
  - Operation tracking
  - Error reporting
  - Performance monitoring
  - Debug information
  - Audit trails

## License

MIT License - see LICENSE file for details

## Support

For questions, issues, or contributions:

1. Check the documentation
   - User guide
   - API reference
   - Examples
   - Troubleshooting
   - FAQ

2. Review existing issues
   - Bug reports
   - Feature requests
   - Known issues
   - Workarounds
   - Solutions

3. Create a new issue
   - Problem description
   - Steps to reproduce
   - Expected behavior
   - Actual behavior
   - Environment details

4. Follow the contributing guidelines
   - Code style
   - Testing
   - Documentation
   - Review process
   - Release process

## Roadmap

### Version 2.0 Features

- [ ] Real-time streaming support
  - Kafka integration
  - Stream processing
  - Real-time validation
  - Stream monitoring
  - Error handling

- [ ] MLlib integration
  - Anomaly detection
  - Pattern recognition
  - Predictive analytics
  - Model training
  - Model serving

- [ ] Web-based configuration UI
  - Visual configuration
  - Real-time validation
  - Template management
  - User management
  - Access control

- [ ] Integration with data catalogs
  - Metadata management
  - Schema registry
  - Data lineage
  - Quality metrics
  - Access control

- [ ] Advanced visualization dashboards
  - Real-time metrics
  - Performance monitoring
  - Quality scores
  - Error tracking
  - Resource usage

- [ ] Custom validation rule engine
  - Rule definition
  - Rule execution
  - Rule management
  - Rule testing
  - Rule deployment

- [ ] Multi-format support
  - JSON
  - CSV
  - Avro
  - XML
  - Custom formats

- [ ] Cloud storage optimizations
  - S3
  - GCS
  - Azure
  - Cloud monitoring
  - Cost optimization

### Performance Improvements

- [ ] Columnar processing optimizations
  - Vectorized operations
  - Memory efficiency
  - Cache optimization
  - Compression
  - Partitioning

- [ ] Delta Lake integration
  - ACID transactions
  - Schema evolution
  - Time travel
  - Upserts
  - Deletes

- [ ] Intelligent sampling strategies
  - Adaptive sampling
  - Stratified sampling
  - Random sampling
  - Systematic sampling
  - Cluster sampling

- [ ] Predictive batch sizing
  - Resource prediction
  - Load balancing
  - Dynamic adjustment
  - Performance optimization
  - Cost optimization

- [ ] Advanced caching strategies
  - Multi-level cache
  - Cache invalidation
  - Cache persistence
  - Cache statistics
  - Cache optimization

# Legal Domain Filtering

This module provides functionality for filtering and classifying legal domain content using the BERTurk-Legal model. It is designed to work with PySpark DataFrames and provides both keyword-based and model-based classification approaches.

## Features

- Legal domain classification using BERTurk-Legal model
- Keyword-based pre-filtering for improved performance
- Caching mechanism for model predictions
- Configurable threshold and batch size
- GPU support with automatic device selection
- Comprehensive error handling and logging
- Detailed statistics and metrics

## Installation

1. Install the required dependencies:
```bash
pip install -r requirements.txt
```

2. The BERTurk-Legal model will be automatically downloaded on first use.

## Usage

### Basic Usage

```python
from pyspark.sql import SparkSession
from src.data_quality.utils.legal_domain_filter import LegalDomainFilter

# Initialize Spark session
spark = SparkSession.builder \
    .appName("LegalDomainFilter") \
    .getOrCreate()

# Configure the filter
config = {
    "model_name": "KocLab-Bilkent/BERTurk-Legal",
    "cache_dir": "./model_cache",
    "threshold": 0.5,
    "batch_size": 32,
    "device": "auto"  # or "cpu" or "cuda"
}

# Create filter instance
filter = LegalDomainFilter(config)

# Process DataFrame
df = spark.read.parquet("path/to/data.parquet")
result_df, stats = filter.process(df, "text_column")

# View results
result_df.show()
print(stats)
```

### Configuration Options

- `model_name`: Name of the model to use (default: "KocLab-Bilkent/BERTurk-Legal")
- `cache_dir`: Directory to cache the model (default: "./model_cache")
- `threshold`: Probability threshold for legal domain classification (default: 0.5)
- `batch_size`: Batch size for processing (default: 32)
- `device`: Device to use for inference ("auto", "cpu", or "cuda")

### Output

The `process` method returns a tuple containing:

1. Processed DataFrame with additional columns:
   - `is_legal_domain`: Boolean indicating if the text is legal domain
   - `legal_probability`: Probability score from the model

2. Statistics dictionary containing:
   - `total_documents`: Total number of documents processed
   - `legal_documents`: Number of documents classified as legal
   - `legal_percentage`: Percentage of legal documents
   - `model_name`: Name of the model used
   - `threshold`: Classification threshold used
   - `device`: Device used for inference

## Error Handling

The module provides specific exceptions for different error cases:

- `ModelLoadError`: Raised when the model fails to load
- `InferenceError`: Raised when model inference fails
- `ValidationError`: Raised when input validation fails

## Testing

Run the test suite:

```bash
python -m unittest tests/test_legal_domain_filter.py
```

## Performance Considerations

- The module uses a two-stage approach:
  1. Fast keyword matching for obvious legal content
  2. BERT model inference for uncertain cases
- Model predictions are cached to avoid redundant computations
- Batch processing is supported for efficient handling of large datasets
- GPU acceleration is available when CUDA is available

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 
