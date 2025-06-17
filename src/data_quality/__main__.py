"""
Main entry point for the Data Quality Framework
"""

import argparse
import logging
import json
from typing import List
from pyspark.sql import SparkSession

from .core import DataQualityFramework
from .utils.config_validator import ConfigurationValidator

def setup_logging(log_level: str = 'INFO', log_file: str = None):
    """Set up logging configuration"""
    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def load_config(config_path: str) -> dict:
    """Load configuration from JSON file"""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate configuration
        is_valid, errors = ConfigurationValidator.validate_config(config)
        if not is_valid:
            raise ValueError(f"Invalid configuration: {', '.join(errors)}")
        
        return config
    except Exception as e:
        raise ValueError(f"Failed to load configuration: {str(e)}")

def create_sample_configs():
    """Create sample configuration files for different domains"""
    sample_configs = {
        'ecommerce': {
            'checkpoint_dir': '/tmp/dq_checkpoints',
            'output_dir': '/tmp/dq_output',
            'batch_size': 1000000,
            'missing_value_strategy': 'fill',
            'critical_columns': ['order_id', 'customer_id', 'product_id'],
            'fill_values': {
                'discount': 0.0,
                'shipping_cost': 0.0
            },
            'mandatory_fields': ['order_id', 'customer_id', 'order_date'],
            'numerical_columns': ['price', 'quantity', 'discount'],
            'decimal_places': 2,
            'unique_constraints': [
                {
                    'columns': ['order_id'],
                    'action': 'drop_duplicates'
                }
            ]
        },
        'financial': {
            'checkpoint_dir': '/tmp/dq_checkpoints',
            'output_dir': '/tmp/dq_output',
            'batch_size': 1000000,
            'missing_value_strategy': 'drop',
            'critical_columns': ['transaction_id', 'account_id', 'amount'],
            'mandatory_fields': ['transaction_id', 'account_id', 'amount', 'transaction_date'],
            'numerical_columns': ['amount', 'balance'],
            'decimal_places': 2,
            'date_columns': ['transaction_date'],
            'data_retention_days': 365
        },
        'healthcare': {
            'checkpoint_dir': '/tmp/dq_checkpoints',
            'output_dir': '/tmp/dq_output',
            'batch_size': 1000000,
            'missing_value_strategy': 'drop',
            'critical_columns': ['patient_id', 'visit_id'],
            'mandatory_fields': ['patient_id', 'visit_id', 'visit_date'],
            'numerical_columns': ['temperature', 'blood_pressure', 'heart_rate'],
            'decimal_places': 1,
            'date_columns': ['visit_date', 'birth_date'],
            'data_retention_days': 730
        }
    }
    
    for domain, config in sample_configs.items():
        filename = f"{domain}_data_quality_config.json"
        with open(filename, 'w') as f:
            json.dump(config, f, indent=2)
        print(f"Created sample configuration: {filename}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Data Quality Framework')
    parser.add_argument('input_files', nargs='*', help='Input parquet files to process')
    parser.add_argument('--config', help='Path to configuration file')
    parser.add_argument('--output-dir', help='Output directory for processed files')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                      help='Logging level')
    parser.add_argument('--log-file', help='Path to log file')
    parser.add_argument('--create-sample-configs', action='store_true',
                      help='Create sample configuration files')
    parser.add_argument('--analyze-only', action='store_true',
                      help='Only analyze data without processing')
    
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    if args.create_sample_configs:
        create_sample_configs()
        return
    
    if not args.input_files:
        parser.error("No input files specified")
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("Data Quality Framework") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        # Load configuration
        config = load_config(args.config) if args.config else {
            'checkpoint_dir': '/tmp/dq_checkpoints',
            'output_dir': args.output_dir or '/tmp/dq_output',
            'batch_size': 1000000
        }
        
        # Initialize framework
        dq_framework = DataQualityFramework(spark, config)
        
        # Process files
        results = dq_framework.process_parquet_files(args.input_files)
        
        # Print summary
        print("\n" + "="*50)
        print("DATA QUALITY PROCESSING SUMMARY")
        print("="*50)
        print(f"Files Processed: {results['validation_summary']['total_files_processed']}")
        print(f"Files Failed: {results['validation_summary']['total_files_failed']}")
        print(f"Total Records Processed: {results['validation_summary']['total_records_processed']:,}")
        print(f"Total Records Cleaned: {results['validation_summary']['total_records_cleaned']:,}")
        print(f"Data Quality Improvement: {results['validation_summary']['data_quality_improvement']:.2f}%")
        
        # Print metrics summary
        print("\n" + "="*50)
        print("PERFORMANCE METRICS SUMMARY")
        print("="*50)
        metrics_summary = results['metrics_summary']
        print(f"Total Processing Time: {metrics_summary['total_processing_time']:.2f} seconds")
        print("\nAverage Processing Times:")
        for op, time in metrics_summary['average_processing_times'].items():
            print(f"  {op}: {time:.2f} seconds")
        print("\nPeak Memory Usage:")
        for op, memory in metrics_summary['peak_memory_usage'].items():
            print(f"  {op}: {memory / (1024*1024):.2f} MB")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main() 