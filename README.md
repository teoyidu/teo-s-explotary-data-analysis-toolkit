# PySpark Data Quality Framework

A comprehensive, production-ready data quality framework for processing Parquet files using Apache Spark. This framework implements 10 essential data quality checks with batch processing capabilities, error handling, and configurable checkpointing.

## Features

### 🔍 Comprehensive Data Quality Checks

The framework implements 10 key data quality validation steps:

1. **F1: Check for Missing Values** - Identify and handle null/missing values
2. **F2: Ensure Mandatory Fields Populated** - Validate required fields are present
3. **F3: Standardize Numerical Data Formats** - Normalize numeric data formats
4. **F4: Identify & Remove Outdated/Irrelevant Data** - Filter based on date ranges and business rules
5. **F5: Validate Data Against External Sources** - Cross-reference with lookup tables
6. **F6: Confirm Data Uniqueness** - Remove duplicates and enforce uniqueness constraints
7. **F7: Match Data Entries to Correct Categories** - Standardize categorical data
8. **F8: Validate Accuracy of Text Fields** - Apply regex patterns and text validation rules
9. **F9: Ensure Proper Data Relationships** - Validate foreign keys and conditional relationships
10. **F10: Implement Data Entry Rules** - Apply business rules and value constraints

### 🚀 Performance & Scalability

- **Batch Processing**: Efficiently handle large datasets by processing in configurable batches
- **Concurrent Processing**: Support for parallel batch execution
- **Memory Optimization**: Intelligent caching and memory management
- **Adaptive Query Execution**: Leverages Spark's adaptive query optimization
- **Checkpointing**: Resume processing from failure points

### ⚙️ Configuration & Management

- **Flexible Configuration**: JSON/YAML configuration files
- **Environment-Specific Configs**: Support for dev/test/prod environments
- **Domain-Specific Templates**: Pre-built configurations for e-commerce, finance, healthcare
- **Validation**: Built-in configuration validation

### 🔧 Enterprise Features

- **Comprehensive Logging**: Structured logging with multiple levels
- **Error Handling**: Robust error handling with detailed error reporting
- **Monitoring**: Processing statistics and performance metrics
- **Recovery**: Checkpoint-based recovery mechanisms

## Installation

### Prerequisites

- Python 3.8+
- Apache Spark 3.4+
- Java 8 or 11

### Install Dependencies

```bash
pip install -r requirements.txt
```

### For Distributed Deployment

```bash
# Install on all cluster nodes
pip install pyspark pandas pyarrow PyYAML
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
  
  "missing_value_strategy": "fill",
  "missing_threshold": 30.0,
  "critical_columns": ["order_id", "customer_id", "product_id"],
  "fill_values": {
    "discount": 0.0,
    "shipping_cost": 0.0
  },
  
  "mandatory_fields": ["order_id", "customer_id", "order_date"],
  
  "numerical_columns": ["price", "quantity", "discount"],
  "decimal_places": 2,
  
  "unique_constraints": [
    {
      "columns": ["order_id"],
      "action": "drop_duplicates"
    }
  ],
  
  "text_validation_rules": {
    "email": {
      "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
      "min_length": 5,
      "max_length": 100
    }
  },
  
  "entry_rules": [
    {
      "name": "price_validation",
      "type": "range_check",
      "column": "price",
      "min_value": 0.01,
      "max_value": 100000.0
    }
  ]
}
```

### Configuration Sections

#### General Settings
- `checkpoint_dir`: Directory for storing checkpoints
- `output_dir`: Directory for processed data
- `batch_size`: Number of records per batch

#### F1: Missing Values
- `missing_value_strategy`: 'drop' or 'fill'
- `missing_threshold`: Percentage threshold for dropping columns
- `critical_columns`: Columns that cannot have missing values
- `fill_values`: Default values for filling missing data

#### F2: Mandatory Fields
- `mandatory_fields`: List of required columns

#### F3: Numerical Standardization
- `numerical_columns`: Columns to standardize
- `decimal_places`: Number of decimal places

#### F6: Uniqueness
- `unique_constraints`: Duplicate removal rules

#### F8: Text Validation
- `text_validation_rules`: Regex patterns and length constraints

#### F10: Entry Rules
- `entry_rules`: Business rules and value constraints

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

print(f"Processed {results['validation_summary']['total_files_processed']} files")
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

### Environment-Specific Configuration

```python
from config_manager import ConfigurationManager

# Create base configuration
base_config = ConfigurationManager('base_config.json')

# Create environment-specific versions
dev_config = base_config.create_environment_specific_config('dev')
prod_config = base_config.create_environment_specific_config('prod')

# Save environment configs
dev_config.save_config('dev_config.json')
prod_config.save_config('prod_config.json')
```

## Command Line Interface

### Basic Commands

```bash
# Show help
python main.py --help

# Process with specific log level
python main.py data.parquet --log-level DEBUG

# Custom log file
python main.py data.parquet --log-file my_process.log

# Specify output directory
python main.py data.parquet --output-dir /path/to/output
```

### Batch Processing Options

```bash
# Enable batch processing (default)
python main.py data.parquet --batch-processing

# Disable batch processing
python main.py data.parquet --no-batch-processing

# Analysis only (no processing)
python main.py data.parquet --analyze-only
```

## Architecture

### Framework Components

```
┌─────────────────────────────────────────┐
│                 main.py                 │
│         (CLI & Orchestration)           │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│         DataQualityFramework            │
│     (Core Processing Engine)            │
│                                         │
│  F1: Missing Values                     │
│  F2: Mandatory Fields                   │
│  F3: Numerical Standardization          │
│  F4: Outdated Data Removal              │
│  F5: External Source Validation         │
│  F6: Data Uniqueness                    │
│  F7: Category Matching                  │
│  F8: Text Field Validation              │
│  F9: Data Relationships                 │
│  F10: Entry Rules                       │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│           BatchProcessor                │
│    (Large Dataset Handling)             │
│                                         │
│  • Batch Creation                       │
│  • Concurrent Processing                │
│  • Memory Optimization                  │
│  • Checkpointing                        │
│  • Result Merging                       │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│        ConfigurationManager             │
│      (Configuration Handling)           │
│                                         │
│  • JSON/YAML Loading                    │
│  • Validation                           │
│  • Environment-Specific Configs         │
│  • Domain Templates                     │
└─────────────────────────────────────────┘
```

### Data Flow

```
Input Parquet Files
        │
        ▼
   Validation & Loading
        │
        ▼
   Batch Creation (if enabled)
        │
        ▼
   Data Quality Pipeline
        │
   ┌────▼────┐
   │   F1    │ Check Missing Values
   └────┬────┘
   ┌────▼────┐
   │   F2    │ Mandatory Fields
   └────┬────┘
   ┌────▼────┐
   │   F3    │ Numerical Standardization
   └────┬────┘
        │
       ...
        │
   ┌────▼────┐
   │   F10   │ Entry Rules
   └────┬────┘
        │
        ▼
   Checkpointing & Saving
        │
        ▼
   Results Aggregation
        │
        ▼
   Output Parquet Files + Statistics
```

## Performance Tuning

### Spark Configuration

The framework automatically sets optimal Spark configurations:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

### Batch Size Recommendations

| Dataset Size | Recommended Batch Size | Concurrent Batches |
|-------------|----------------------|-------------------|
| < 100K rows | Process all at once | 1 |
| 100K - 1M rows | 100K | 2 |
| 1M - 10M rows | 500K | 3 |
| > 10M rows | 1M | 4 |

### Memory Optimization

- Enable memory optimization for datasets > 1GB
- Use checkpoints for long-running processes
- Configure appropriate partition sizes

## Error Handling

### Types of Errors Handled

1. **File I/O Errors**: Missing files, permission issues
2. **Schema Validation Errors**: Unexpected data types, missing columns
3. **Data Quality Errors**: Constraint violations, invalid data
4. **Processing Errors**: Memory issues, timeout errors
5. **Configuration Errors**: Invalid settings, missing parameters

### Error Recovery

- Automatic retry for transient errors
- Checkpoint-based recovery for batch processing
- Detailed error logging and reporting
- Graceful degradation for non-critical errors

## Monitoring & Observability

### Metrics Collected

- Processing throughput (records/second)
- Data quality improvement percentage
- Memory usage patterns
- Processing time per validation step
- Error rates and types

### Logging Levels

- **DEBUG**: Detailed processing information
- **INFO**: General processing progress
- **WARNING**: Non-critical issues
- **ERROR**: Processing failures
- **CRITICAL**: System-level failures

### Output Reports

The framework generates comprehensive reports:

```json
{
  "summary": {
    "total_files": 5,
    "successful_files": 4,
    "failed_files": 1,
    "success_rate": 80.0,
    "total_processing_time_minutes": 12.5
  },
  "validation_summary": {
    "f1_missing_values": {"rows_dropped": 150},
    "f2_mandatory_fields": {"rows_removed": 25},
    "f6_uniqueness": {"total_duplicates_removed": 1200}
  }
}
```

## Contributing

### Setting Up Development Environment

```bash
# Clone repository
git clone <repository-url>
cd pyspark-data-quality-framework

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/

# Run with sample data
python main.py --create-sample-configs
python main.py sample_data.parquet --config ecommerce_data_quality_config.json
```

### Code Style

- Follow PEP 8 guidelines
- Use type hints for function parameters and return values
- Include docstrings for all public methods
- Add logging for important operations

## License

MIT License - see LICENSE file for details

## Support

For questions, issues, or contributions:

1. Check the documentation
2. Review existing issues
3. Create a new issue with detailed information
4. Follow the contributing guidelines

## Roadmap

### Version 2.0 Features

- [ ] Real-time streaming support
- [ ] MLlib integration for anomaly detection
- [ ] Web-based configuration UI
- [ ] Integration with data catalogs
- [ ] Advanced visualization dashboards
- [ ] Custom validation rule engine
- [ ] Multi-format support (JSON, CSV, Avro)
- [ ] Cloud storage optimizations (S3, GCS, Azure)

### Performance Improvements

- [ ] Columnar processing optimizations
- [ ] Delta Lake integration
- [ ] Intelligent sampling strategies
- [ ] Predictive batch sizing
- [ ] Advanced caching strategies 