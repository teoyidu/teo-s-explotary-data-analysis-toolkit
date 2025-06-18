#!/usr/bin/env python3
"""
PySpark Data Quality Framework
Comprehensive data validation and cleansing pipeline for Parquet files
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable
import json
import time
import psutil
import threading
import unittest
from unittest.mock import Mock, patch
import tempfile
import shutil
import jsonschema
from jsonschema import validate
from jsonschema.exceptions import ValidationError as JSONSchemaValidationError

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, isnan, isnull, when, count, sum as spark_sum, avg, stddev,
    regexp_replace, trim, upper, lower, length, split, array_contains,
    to_date, to_timestamp, datediff, current_date, current_timestamp,
    row_number, rank, dense_rank, percent_rank, ntile, lag, lead,
    collect_list, collect_set, size, explode, posexplode,
    coalesce, greatest, least, abs as spark_abs, round as spark_round,
    format_number, concat, concat_ws, substring, instr, locate,
    regexp_extract, regexp_extract_all, split as spark_split,
    levenshtein, initcap, element_at, add_months
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType
)
from pyspark.storagelevel import StorageLevel
from src.legal_domain_filter import LegalDomainFilter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_quality.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataQualityError(Exception):
    """Base exception for data quality framework errors"""
    pass

class ConfigurationError(DataQualityError):
    """Raised when there are configuration issues"""
    pass

class ValidationError(DataQualityError):
    """Raised when data validation fails"""
    pass

class ProcessingError(DataQualityError):
    """Raised when data processing fails"""
    pass

class ExternalValidationError(DataQualityError):
    """Raised when external validation fails"""
    pass

class ConfigurationValidator:
    """Validates configuration for the data quality framework"""
    
    CONFIG_SCHEMA = {
        "type": "object",
        "required": ["checkpoint_dir", "output_dir", "batch_size"],
        "properties": {
            "checkpoint_dir": {"type": "string"},
            "output_dir": {"type": "string"},
            "batch_size": {"type": "integer", "minimum": 1},
            "missing_value_strategy": {
                "type": "string",
                "enum": ["drop", "fill"]
            },
            "missing_threshold": {
                "type": "number",
                "minimum": 0,
                "maximum": 100
            },
            "critical_columns": {
                "type": "array",
                "items": {"type": "string"}
            },
            "fill_values": {
                "type": "object",
                "additionalProperties": True
            },
            "mandatory_fields": {
                "type": "array",
                "items": {"type": "string"}
            },
            "numerical_columns": {
                "type": "array",
                "items": {"type": "string"}
            },
            "decimal_places": {
                "type": "integer",
                "minimum": 0
            },
            "date_columns": {
                "type": "array",
                "items": {"type": "string"}
            },
            "data_retention_days": {
                "type": "integer",
                "minimum": 1
            },
            "irrelevant_data_conditions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["column", "values"],
                    "properties": {
                        "column": {"type": "string"},
                        "values": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    }
                }
            },
            "reference_data": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["path", "key_column", "df_column"],
                    "properties": {
                        "path": {"type": "string"},
                        "key_column": {"type": "string"},
                        "df_column": {"type": "string"}
                    }
                }
            },
            "unique_constraints": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["columns"],
                    "properties": {
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "action": {
                            "type": "string",
                            "enum": ["drop_duplicates", "keep_first", "keep_latest"]
                        },
                        "order_by": {"type": "string"},
                        "similarity_threshold": {"type": "number", "minimum": 0, "maximum": 1},
                        "fuzzy_matching": {"type": "boolean"}
                    }
                }
            },
            "category_mappings": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "additionalProperties": {"type": "string"}
                }
            },
            "text_standardization": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "trim": {"type": "boolean"},
                        "case": {
                            "type": "string",
                            "enum": ["upper", "lower", "title"]
                        }
                    }
                }
            },
            "text_validation_rules": {
                "type": "object",
                "additionalProperties": {
                    "type": "object",
                    "properties": {
                        "pattern": {"type": "string"},
                        "min_length": {"type": "integer", "minimum": 0},
                        "max_length": {"type": "integer", "minimum": 0},
                        "remove_special_chars": {"type": "boolean"},
                        "clean_text": {"type": "boolean"},
                        "case_normalization": {
                            "type": "string",
                            "enum": ["none", "lower", "upper", "title"]
                        },
                        "transformations": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "enum": ["replace", "extract", "split"]},
                                    "pattern": {"type": "string"},
                                    "replacement": {"type": "string"},
                                    "delimiter": {"type": "string"},
                                    "index": {"type": "integer"}
                                }
                            }
                        },
                        "filter_profanity": {"type": "boolean"},
                        "mask_sensitive_data": {"type": "boolean"},
                        "mask_pattern": {"type": "string"}
                    }
                }
            },
            "relationship_rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["type"],
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["foreign_key", "conditional"]
                        },
                        "child_column": {"type": "string"},
                        "parent_table_path": {"type": "string"},
                        "parent_column": {"type": "string"},
                        "condition_column": {"type": "string"},
                        "condition_value": {"type": "boolean"},
                        "dependent_column": {"type": "string"},
                        "dependent_required": {"type": "boolean"}
                    }
                }
            },
            "entry_rules": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["name", "type"],
                    "properties": {
                        "name": {"type": "string"},
                        "type": {
                            "type": "string",
                            "enum": ["range_check", "allowed_values", "cross_field_validation"]
                        },
                        "column": {"type": "string"},
                        "min_value": {"type": "number"},
                        "max_value": {"type": "number"},
                        "allowed_values": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "field1": {"type": "string"},
                        "field2": {"type": "string"},
                        "operator": {
                            "type": "string",
                            "enum": [">", "<", ">=", "<=", "==", "!="]
                        }
                    }
                }
            },
            "text_column": {
                "type": "string",
                "description": "Name of the column containing text to analyze for legal domain filtering"
            },
            "legal_domain_filtering": {
                "type": "object",
                "required": ["enabled", "threshold", "text_column"],
                "properties": {
                    "enabled": {
                        "type": "boolean",
                        "description": "Whether to enable legal domain filtering"
                    },
                    "threshold": {
                        "type": "number",
                        "minimum": 0,
                        "maximum": 1,
                        "description": "Probability threshold for legal domain classification"
                    },
                    "text_column": {
                        "type": "string",
                        "description": "Name of the column containing text to analyze"
                    },
                    "model_name": {
                        "type": "string",
                        "description": "Name of the model to use for legal domain classification",
                        "default": "KocLab-Bilkent/BERTurk-Legal"
                    },
                    "cache_dir": {
                        "type": "string",
                        "description": "Directory to cache the model",
                        "default": "./model_cache"
                    },
                    "batch_size": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Batch size for processing",
                        "default": 32
                    },
                    "device": {
                        "type": "string",
                        "enum": ["auto", "cpu", "cuda"],
                        "description": "Device to use for inference",
                        "default": "auto"
                    }
                }
            }
        }
    }
    
    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate configuration against schema
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        try:
            validate(instance=config, schema=cls.CONFIG_SCHEMA)
            return True, []
        except JSONSchemaValidationError as e:
            return False, [str(e)]
        except Exception as e:
            return False, [f"Unexpected error during validation: {str(e)}"]

class MetricsCollector:
    """Collects and manages metrics for the data quality framework"""
    
    def __init__(self):
        self.metrics = {}
        self.start_times = {}
        self.memory_threshold = 0.85  # 85% memory usage threshold
        self.last_cleanup_time = time.time()
        self.cleanup_interval = 300  # 5 minutes
    
    def start_timer(self, operation: str) -> float:
        """Start timing an operation"""
        start_time = time.time()
        self.start_times[operation] = start_time
        return start_time
    
    def end_timer(self, operation: str, start_time: float):
        """End timing an operation and record duration"""
        if operation in self.start_times:
            duration = time.time() - start_time
            if 'durations' not in self.metrics:
                self.metrics['durations'] = {}
            self.metrics['durations'][operation] = duration
            del self.start_times[operation]
    
    def record_memory_usage(self, operation: str):
        """Record memory usage for an operation"""
        process = psutil.Process()
        memory_info = process.memory_info()
        
        if 'memory_usage' not in self.metrics:
            self.metrics['memory_usage'] = {}
        
        self.metrics['memory_usage'][operation] = {
            'rss': memory_info.rss,  # Resident Set Size
            'vms': memory_info.vms,  # Virtual Memory Size
            'percent': process.memory_percent()
        }
        
        # Check if memory pressure is high
        if process.memory_percent() > self.memory_threshold * 100:
            self._handle_memory_pressure()
    
    def _handle_memory_pressure(self):
        """Handle high memory pressure situations"""
        current_time = time.time()
        
        # Only perform cleanup if enough time has passed since last cleanup
        if current_time - self.last_cleanup_time > self.cleanup_interval:
            logger.warning("High memory pressure detected, performing cleanup...")
            
            # Force garbage collection
            import gc
            gc.collect()
            
            # Clear any cached data
            if 'cached_data' in self.metrics:
                del self.metrics['cached_data']
            
            # Update last cleanup time
            self.last_cleanup_time = current_time
            
            # Log memory status after cleanup
            process = psutil.Process()
            logger.info(f"Memory usage after cleanup: {process.memory_percent():.1f}%")
    
    def record_record_count(self, operation: str, count: int):
        """Record the number of records processed"""
        if 'record_counts' not in self.metrics:
            self.metrics['record_counts'] = {}
        self.metrics['record_counts'][operation] = count
    
    def record_validation_stats(self, operation: str, stats: Dict):
        """Record validation statistics"""
        if 'validation_stats' not in self.metrics:
            self.metrics['validation_stats'] = {}
        self.metrics['validation_stats'][operation] = stats
    
    def get_metrics(self) -> Dict:
        """Get all collected metrics"""
        return self.metrics
    
    def get_summary(self) -> Dict:
        """Get a summary of the metrics"""
        summary = {
            'total_duration': sum(self.metrics.get('durations', {}).values()),
            'total_records': sum(self.metrics.get('record_counts', {}).values()),
            'memory_usage': {
                'current': psutil.Process().memory_percent(),
                'peak': max(m['percent'] for m in self.metrics.get('memory_usage', {}).values())
            },
            'validation_summary': {
                'total_validations': len(self.metrics.get('validation_stats', {})),
                'failed_validations': sum(1 for stats in self.metrics.get('validation_stats', {}).values()
                                       if stats.get('status') == 'failed')
            }
        }
        return summary

class DataQualityFramework:
    """
    Comprehensive data quality framework for PySpark DataFrame processing
    
    This framework provides a robust solution for data quality validation and cleansing
    of Parquet files using PySpark. It includes the following features:
    
    1. Missing value detection and handling
    2. Mandatory field validation
    3. Numerical format standardization
    4. Outdated data removal
    5. External source validation
    6. Data uniqueness confirmation
    7. Category mapping and standardization
    8. Text field validation
    9. Relationship validation
    10. Data entry rule enforcement
    
    The framework also includes:
    - Performance monitoring and metrics collection
    - Comprehensive error handling
    - Checkpointing for fault tolerance
    - Batch processing capabilities
    - Detailed logging and reporting
    
    Example usage:
    ```python
    from pyspark.sql import SparkSession
    from data_quality_framework import DataQualityFramework, create_sample_config
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("Data Quality Framework").getOrCreate()
    
    # Create configuration
    config = create_sample_config()
    
    # Initialize framework
    dq_framework = DataQualityFramework(spark, config)
    
    # Process files
    results = dq_framework.process_parquet_files(["/path/to/your/data.parquet"])
    ```
    """
    
    def __init__(self, spark_session: SparkSession, config: Dict[str, Any]):
        """
        Initialize the Data Quality Framework
        
        Args:
            spark_session: Active Spark session
            config: Configuration dictionary containing validation rules and settings
            
        Raises:
            ConfigurationError: If the configuration is invalid
        """
        # Validate configuration
        is_valid, errors = ConfigurationValidator.validate_config(config)
        if not is_valid:
            raise ConfigurationError(f"Invalid configuration: {', '.join(errors)}")
        
        self.spark = spark_session
        self.config = config
        self.checkpoint_dir = config.get('checkpoint_dir', '/tmp/dq_checkpoints')
        self.output_dir = config.get('output_dir', '/tmp/dq_output')
        self.batch_size = config.get('batch_size', 1000000)
        self.validation_results = {}
        self.metrics = MetricsCollector()
        
        # Create directories if they don't exist
        self._create_directories()
        
        # Set Spark configurations for optimization
        self._configure_spark()
    
    def _create_directories(self):
        """Create necessary directories for checkpoints and output"""
        try:
            # For local filesystem
            os.makedirs(self.checkpoint_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Created directories: {self.checkpoint_dir}, {self.output_dir}")
        except Exception as e:
            logger.warning(f"Could not create local directories: {e}")
    
    def _configure_spark(self):
        """Configure Spark session for optimal performance"""
        # Enable adaptive query execution
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        # Enable vectorized parquet reading
        self.spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
        self.spark.conf.set("spark.sql.parquet.columnarReaderBatchSize", "4096")
        
        # Enable Arrow optimization
        self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        
        # Memory and shuffle optimizations
        self.spark.conf.set("spark.memory.fraction", "0.8")
        self.spark.conf.set("spark.memory.storageFraction", "0.3")
        self.spark.conf.set("spark.shuffle.file.buffer", "1m")
        self.spark.conf.set("spark.file.transferTo", "true")
        
        # Compression settings
        self.spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
        self.spark.conf.set("spark.shuffle.compress", "true")
        
        # Dynamic partition pruning
        self.spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
        
        # Join optimizations
        self.spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
        self.spark.conf.set("spark.sql.shuffle.partitions", "200")
        
        logger.info("Spark session configured with performance optimizations")
    
    def process_parquet_files(self, input_paths: List[str]) -> Dict[str, Any]:
        """
        Main method to process multiple parquet files with data quality checks
        
        Args:
            input_paths: List of parquet file paths to process
            
        Returns:
            Dictionary containing processing results and statistics
        """
        results = {
            'processed_files': [],
            'failed_files': [],
            'total_records_processed': 0,
            'total_records_cleaned': 0,
            'validation_summary': {}
        }
        
        logger.info(f"Starting processing of {len(input_paths)} parquet files")
        
        for i, path in enumerate(input_paths):
            try:
                logger.info(f"Processing file {i+1}/{len(input_paths)}: {path}")
                
                # Load parquet file
                df = self._load_parquet_with_validation(path)
                if df is None:
                    results['failed_files'].append({'path': path, 'error': 'Failed to load'})
                    continue
                
                original_count = df.count()
                logger.info(f"Loaded {original_count} records from {path}")
                
                # Apply all data quality checks
                cleaned_df, validation_stats = self._apply_data_quality_pipeline(df, path)
                
                # Save cleaned data with checkpoint
                output_path = self._save_with_checkpoint(cleaned_df, path, i)
                
                cleaned_count = cleaned_df.count()
                
                # Update results
                results['processed_files'].append({
                    'path': path,
                    'output_path': output_path,
                    'original_count': original_count,
                    'cleaned_count': cleaned_count,
                    'validation_stats': validation_stats
                })
                
                results['total_records_processed'] += original_count
                results['total_records_cleaned'] += cleaned_count
                
                logger.info(f"Successfully processed {path}: {original_count} -> {cleaned_count} records")
                
            except Exception as e:
                logger.error(f"Failed to process {path}: {str(e)}")
                results['failed_files'].append({'path': path, 'error': str(e)})
                continue
        
        # Generate summary statistics
        results['validation_summary'] = self._generate_summary_statistics(results)
        
        # Save processing results
        self._save_processing_results(results)
        
        logger.info(f"Processing complete. Processed {len(results['processed_files'])} files successfully")
        return results
    
    def _load_parquet_with_validation(self, path: str) -> Optional[DataFrame]:
        """Load parquet file with basic validation"""
        try:
            df = self.spark.read.parquet(path)
            
            # Basic validation
            if df.count() == 0:
                logger.warning(f"Empty parquet file: {path}")
                return None
                
            logger.info(f"Schema for {path}: {df.schema}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading parquet file {path}: {str(e)}")
            raise ProcessingError(f"Failed to load parquet file {path}: {str(e)}")
    
    def _apply_data_quality_pipeline(self, df: DataFrame, file_path: str) -> Tuple[DataFrame, Dict]:
        """Apply all data quality checks in sequence"""
        validation_stats = {}
        current_df = df
        
        try:
            # F1: Check for Missing Values
            logger.info("F1: Checking for missing values")
            start_time = self.metrics.start_timer('f1_missing_values')
            current_df, stats = self.f1_check_missing_values(current_df)
            self.metrics.end_timer('f1_missing_values', start_time)
            self.metrics.record_memory_usage('f1_missing_values')
            self.metrics.record_record_count('f1_missing_values', current_df.count())
            self.metrics.record_validation_stats('f1_missing_values', stats)
            validation_stats['f1_missing_values'] = stats
            
            # F2: Ensure Mandatory Fields Populated
            logger.info("F2: Ensuring mandatory fields populated")
            current_df, stats = self.f2_ensure_mandatory_fields(current_df)
            validation_stats['f2_mandatory_fields'] = stats
            
            # F3: Standardize Numerical Data Formats
            logger.info("F3: Standardizing numerical data formats")
            current_df, stats = self.f3_standardize_numerical_formats(current_df)
            validation_stats['f3_numerical_formats'] = stats
            
            # F4: Identify & Remove Outdated/Irrelevant Data with enhanced filtering
            logger.info("F4: Identifying and removing outdated/irrelevant data with enhanced filtering")
            current_df, stats = self.f4_remove_outdated_data(current_df)
            validation_stats['f4_outdated_data'] = stats
            
            # F5: Validate Data Against External Sources
            logger.info("F5: Validating data against external sources")
            current_df, stats = self.f5_validate_external_sources(current_df)
            validation_stats['f5_external_validation'] = stats
            
            # F6: Confirm Data Uniqueness with enhanced deduplication
            logger.info("F6: Confirming data uniqueness with enhanced deduplication")
            current_df, stats = self.f6_confirm_uniqueness(current_df)
            validation_stats['f6_uniqueness'] = stats
            
            # F7: Match Data Entries to Correct Categories
            logger.info("F7: Matching data entries to correct categories")
            current_df, stats = self.f7_match_categories(current_df)
            validation_stats['f7_categories'] = stats
            
            # F8: Validate and Clean Text Fields with enhanced filtering and decontamination
            logger.info("F8: Validating and cleaning text fields with enhanced filtering and decontamination")
            current_df, stats = self.f8_validate_text_fields(current_df)
            validation_stats['f8_text_validation'] = stats
            
            # F9: Ensure Proper Data Relationships
            logger.info("F9: Ensuring proper data relationships")
            current_df, stats = self.f9_ensure_relationships(current_df)
            validation_stats['f9_relationships'] = stats
            
            # F10: Implement Data Entry Rules
            logger.info("F10: Implementing data entry rules")
            current_df, stats = self.f10_implement_entry_rules(current_df)
            validation_stats['f10_entry_rules'] = stats
            
            # F11: Filter content based on legal domain using BERTurk-Legal model
            logger.info("F11: Filtering content based on legal domain using BERTurk-Legal model")
            current_df, stats = self.f11_filter_legal_domain(current_df)
            validation_stats['f11_legal_domain_filtering'] = stats
            
            # Cache the final result for better performance
            current_df.cache()
            
            return current_df, validation_stats
            
        except Exception as e:
            logger.error(f"Error in data quality pipeline: {str(e)}")
            # Clean up any cached data
            if current_df.is_cached:
                current_df.unpersist()
            raise
        finally:
            # Clean up original DataFrame if it was cached
            if df.is_cached:
                df.unpersist()
    
    def f1_check_missing_values(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F1: Check for Missing Values"""
        stats = {}
        
        try:
            # Calculate missing value statistics
            total_rows = df.count()
            
            for column in df.columns:
                missing_count = df.filter(col(column).isNull() | isnan(col(column))).count()
                missing_percentage = (missing_count / total_rows) * 100 if total_rows > 0 else 0
                
                stats[column] = {
                    'missing_count': missing_count,
                    'missing_percentage': missing_percentage
                }
                
                logger.info(f"Column {column}: {missing_count} missing values ({missing_percentage:.2f}%)")
            
            # Handle missing values based on configuration
            missing_value_strategy = self.config.get('missing_value_strategy', 'drop')
            missing_threshold = self.config.get('missing_threshold', 50.0)  # 50% threshold
            
            if missing_value_strategy == 'drop':
                # Drop rows with missing values in critical columns
                critical_columns = self.config.get('critical_columns', [])
                if critical_columns:
                    df_cleaned = df.dropna(subset=critical_columns)
                else:
                    df_cleaned = df.dropna()
            elif missing_value_strategy == 'fill':
                # Fill missing values with defaults
                fill_values = self.config.get('fill_values', {})
                df_cleaned = df.fillna(fill_values)
            else:
                df_cleaned = df
            
            stats['rows_dropped'] = total_rows - df_cleaned.count()
            
        except Exception as e:
            logger.error(f"Error in F1 - Check Missing Values: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f2_ensure_mandatory_fields(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F2: Ensure Mandatory Fields Populated"""
        stats = {}
        
        try:
            mandatory_fields = self.config.get('mandatory_fields', [])
            original_count = df.count()
            
            if mandatory_fields:
                # Filter out rows where mandatory fields are null or empty
                filter_condition = None
                
                for field in mandatory_fields:
                    if field in df.columns:
                        field_condition = (col(field).isNotNull() & 
                                         (col(field) != "") & 
                                         (~isnan(col(field))))
                        
                        if filter_condition is None:
                            filter_condition = field_condition
                        else:
                            filter_condition = filter_condition & field_condition
                
                if filter_condition is not None:
                    df_cleaned = df.filter(filter_condition)
                else:
                    df_cleaned = df
            else:
                df_cleaned = df
            
            final_count = df_cleaned.count()
            stats['original_count'] = original_count
            stats['final_count'] = final_count
            stats['rows_removed'] = original_count - final_count
            stats['mandatory_fields_checked'] = mandatory_fields
            
        except Exception as e:
            logger.error(f"Error in F2 - Ensure Mandatory Fields: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f3_standardize_numerical_formats(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F3: Standardize Numerical Data Formats"""
        stats = {}
        
        try:
            numerical_columns = self.config.get('numerical_columns', [])
            decimal_places = self.config.get('decimal_places', 2)
            
            df_cleaned = df
            
            for col_name in numerical_columns:
                if col_name in df.columns:
                    # Remove any non-numeric characters and standardize format
                    df_cleaned = df_cleaned.withColumn(
                        col_name,
                        when(col(col_name).isNotNull(),
                             spark_round(
                                 regexp_replace(col(col_name), "[^0-9.-]", "").cast("double"),
                                 decimal_places
                             )
                        ).otherwise(col(col_name))
                    )
            
            stats['standardized_columns'] = numerical_columns
            stats['decimal_places'] = decimal_places
            
        except Exception as e:
            logger.error(f"Error in F3 - Standardize Numerical Formats: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f4_remove_outdated_data(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F4: Identify & Remove Outdated/Irrelevant Data with enhanced filtering"""
        stats = {}
        
        try:
            date_columns = self.config.get('date_columns', [])
            retention_policies = self.config.get('retention_policies', {})
            irrelevant_conditions = self.config.get('irrelevant_data_conditions', [])
            
            original_count = df.count()
            df_cleaned = df
            
            # Apply retention policies
            for column, policy in retention_policies.items():
                if column in df.columns:
                    policy_type = policy.get('type', 'days')
                    retention_value = policy.get('value', 365)
                    
                    if policy_type == 'days':
                        cutoff_date = current_date() - retention_value
                        df_cleaned = df_cleaned.filter(
                            col(column).isNull() | (col(column) >= cutoff_date)
                        )
                    elif policy_type == 'months':
                        cutoff_date = add_months(current_date(), -retention_value)
                        df_cleaned = df_cleaned.filter(
                            col(column).isNull() | (col(column) >= cutoff_date)
                        )
                    elif policy_type == 'years':
                        cutoff_date = add_months(current_date(), -retention_value * 12)
                        df_cleaned = df_cleaned.filter(
                            col(column).isNull() | (col(column) >= cutoff_date)
                        )
                    
                    stats[f"{column}_retention"] = {
                        'policy_type': policy_type,
                        'retention_value': retention_value,
                        'cutoff_date': str(cutoff_date)
                    }
            
            # Apply date-based filtering for columns without specific retention policies
            for date_col in date_columns:
                if date_col not in retention_policies and date_col in df.columns:
                    default_retention = self.config.get('default_retention_days', 365)
                    cutoff_date = current_date() - default_retention
                    
                    df_cleaned = df_cleaned.filter(
                        col(date_col).isNull() | (col(date_col) >= cutoff_date)
                    )
                    
                    stats[f"{date_col}_default_retention"] = {
                        'retention_days': default_retention,
                        'cutoff_date': str(cutoff_date)
                    }
            
            # Apply business rules for irrelevant data
            for condition in irrelevant_conditions:
                column = condition.get('column')
                values = condition.get('values', [])
                operator = condition.get('operator', 'in')
                
                if column in df.columns:
                    if operator == 'in':
                        df_cleaned = df_cleaned.filter(~col(column).isin(values))
                    elif operator == 'not_in':
                        df_cleaned = df_cleaned.filter(col(column).isin(values))
                    elif operator == 'null':
                        df_cleaned = df_cleaned.filter(col(column).isNotNull())
                    elif operator == 'not_null':
                        df_cleaned = df_cleaned.filter(col(column).isNull())
                    elif operator == 'equals':
                        df_cleaned = df_cleaned.filter(col(column) != values[0])
                    elif operator == 'not_equals':
                        df_cleaned = df_cleaned.filter(col(column) == values[0])
                    
                    stats[f"{column}_irrelevant_removed"] = {
                        'operator': operator,
                        'values': values
                    }
            
            # Apply data quality thresholds
            quality_thresholds = self.config.get('quality_thresholds', {})
            for column, threshold in quality_thresholds.items():
                if column in df.columns:
                    min_value = threshold.get('min')
                    max_value = threshold.get('max')
                    
                    if min_value is not None:
                        df_cleaned = df_cleaned.filter(col(column) >= min_value)
                    if max_value is not None:
                        df_cleaned = df_cleaned.filter(col(column) <= max_value)
                    
                    stats[f"{column}_quality_threshold"] = {
                        'min_value': min_value,
                        'max_value': max_value
                    }
            
            final_count = df_cleaned.count()
            stats['original_count'] = original_count
            stats['final_count'] = final_count
            stats['rows_removed'] = original_count - final_count
            
        except Exception as e:
            logger.error(f"Error in F4 - Remove Outdated Data: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f5_validate_external_sources(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F5: Validate Data Against External Sources"""
        stats = {}
        
        try:
            # Load reference data for validation
            reference_data_configs = self.config.get('reference_data', [])
            df_cleaned = df
            validation_results = {}
            
            for ref_config in reference_data_configs:
                ref_path = ref_config.get('path')
                ref_key = ref_config.get('key_column')
                df_key = ref_config.get('df_column')
                
                if not all([ref_path, ref_key, df_key]):
                    raise ConfigurationError(f"Missing required configuration for external validation: {ref_config}")
                
                if df_key not in df.columns:
                    raise ValidationError(f"Column {df_key} not found in DataFrame")
                
                try:
                    # Load reference data
                    ref_df = self.spark.read.parquet(ref_path)
                    valid_values = ref_df.select(ref_key).distinct().rdd.flatMap(lambda x: x).collect()
                    
                    # Count invalid records
                    invalid_count = df_cleaned.filter(~col(df_key).isin(valid_values)).count()
                    
                    # Filter out invalid records
                    df_cleaned = df_cleaned.filter(col(df_key).isin(valid_values))
                    
                    validation_results[df_key] = {
                        'reference_path': ref_path,
                        'invalid_count': invalid_count,
                        'valid_values_count': len(valid_values)
                    }
                    
                except Exception as e:
                    raise ExternalValidationError(f"Failed to validate against reference data {ref_path}: {str(e)}")
            
            stats['validation_results'] = validation_results
            
        except (ConfigurationError, ValidationError, ExternalValidationError) as e:
            logger.error(f"Error in F5 - Validate External Sources: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
            raise
        
        return df_cleaned, stats
    
    def f6_confirm_uniqueness(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F6: Confirm Data Uniqueness with enhanced deduplication"""
        stats = {}
        
        try:
            unique_constraints = self.config.get('unique_constraints', [])
            original_count = df.count()
            df_cleaned = df
            
            for constraint in unique_constraints:
                columns = constraint.get('columns', [])
                action = constraint.get('action', 'drop_duplicates')
                similarity_threshold = constraint.get('similarity_threshold', 0.8)
                fuzzy_matching = constraint.get('fuzzy_matching', False)
                
                if all(col_name in df.columns for col_name in columns):
                    # Handle fuzzy matching if enabled
                    if fuzzy_matching:
                        # Create a temporary column for fuzzy matching
                        df_cleaned = df_cleaned.withColumn(
                            "fuzzy_key",
                            concat_ws("_", *[lower(col(c)).cast("string") for c in columns])
                        )
                        
                        # Apply fuzzy matching using Levenshtein distance
                        window_spec = Window.partitionBy("fuzzy_key").orderBy(col("fuzzy_key"))
                        df_cleaned = df_cleaned.withColumn(
                            "similarity_score",
                            levenshtein(col("fuzzy_key"), lag("fuzzy_key").over(window_spec))
                        )
                        
                        # Filter based on similarity threshold
                        df_cleaned = df_cleaned.filter(
                            (col("similarity_score").isNull()) | 
                            (col("similarity_score") <= (1 - similarity_threshold) * 100)
                        )
                        
                        # Drop temporary columns
                        df_cleaned = df_cleaned.drop("fuzzy_key", "similarity_score")
                    
                    # Handle exact matching
                    duplicate_count = df_cleaned.count() - df_cleaned.dropDuplicates(columns).count()
                    
                    if action == 'drop_duplicates':
                        df_cleaned = df_cleaned.dropDuplicates(columns)
                    elif action == 'keep_first':
                        # Keep first occurrence based on a timestamp or ID column
                        order_column = constraint.get('order_by')
                        if order_column and order_column in df.columns:
                            window_spec = Window.partitionBy(*columns).orderBy(col(order_column))
                            df_cleaned = df_cleaned.withColumn("row_number", row_number().over(window_spec))
                            df_cleaned = df_cleaned.filter(col("row_number") == 1).drop("row_number")
                    elif action == 'keep_latest':
                        # Keep the most recent record
                        order_column = constraint.get('order_by')
                        if order_column and order_column in df.columns:
                            window_spec = Window.partitionBy(*columns).orderBy(col(order_column).desc())
                            df_cleaned = df_cleaned.withColumn("row_number", row_number().over(window_spec))
                            df_cleaned = df_cleaned.filter(col("row_number") == 1).drop("row_number")
                    
                    stats[f"{'_'.join(columns)}_duplicates"] = duplicate_count
            
            final_count = df_cleaned.count()
            stats['original_count'] = original_count
            stats['final_count'] = final_count
            stats['total_duplicates_removed'] = original_count - final_count
            
        except Exception as e:
            logger.error(f"Error in F6 - Confirm Uniqueness: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f7_match_categories(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F7: Match Data Entries to Correct Categories"""
        stats = {}
        
        try:
            category_mappings = self.config.get('category_mappings', {})
            df_cleaned = df
            mapping_stats = {}
            
            for column, mappings in category_mappings.items():
                if column in df.columns:
                    # Create mapping expression
                    mapping_expr = col(column)
                    for old_value, new_value in mappings.items():
                        mapping_expr = when(col(column) == old_value, new_value).otherwise(mapping_expr)
                    
                    # Count changes
                    changes_count = df_cleaned.filter(col(column).isin(list(mappings.keys()))).count()
                    
                    df_cleaned = df_cleaned.withColumn(column, mapping_expr)
                    mapping_stats[column] = {
                        'mappings_applied': len(mappings),
                        'records_changed': changes_count
                    }
            
            # Standardize text categories
            text_standardization = self.config.get('text_standardization', {})
            for column, rules in text_standardization.items():
                if column in df.columns:
                    if rules.get('trim', True):
                        df_cleaned = df_cleaned.withColumn(column, trim(col(column)))
                    
                    case_rule = rules.get('case', 'upper')  # 'upper', 'lower', 'title'
                    if case_rule == 'upper':
                        df_cleaned = df_cleaned.withColumn(column, upper(col(column)))
                    elif case_rule == 'lower':
                        df_cleaned = df_cleaned.withColumn(column, lower(col(column)))
            
            stats['mapping_stats'] = mapping_stats
            
        except Exception as e:
            logger.error(f"Error in F7 - Match Categories: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f8_validate_text_fields(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F8: Validate and Clean Text Fields with enhanced filtering and decontamination"""
        stats = {}
        
        try:
            text_validation_rules = self.config.get('text_validation_rules', {})
            df_cleaned = df
            
            for column, rules in text_validation_rules.items():
                if column in df.columns:
                    column_stats = {}
                    original_count = df_cleaned.count()
                    
                    # Apply text cleaning rules
                    if rules.get('clean_text', True):
                        # Remove special characters and normalize whitespace
                        df_cleaned = df_cleaned.withColumn(
                            column,
                            regexp_replace(
                                regexp_replace(
                                    trim(col(column)),
                                    r'[^\w\s]',  # Remove special characters
                                    ''
                                ),
                                r'\s+',  # Normalize whitespace
                                ' '
                            )
                        )
                    
                    # Apply case normalization
                    case_rule = rules.get('case_normalization', 'none')
                    if case_rule == 'lower':
                        df_cleaned = df_cleaned.withColumn(column, lower(col(column)))
                    elif case_rule == 'upper':
                        df_cleaned = df_cleaned.withColumn(column, upper(col(column)))
                    elif case_rule == 'title':
                        df_cleaned = df_cleaned.withColumn(column, initcap(col(column)))
                    
                    # Apply length validation
                    min_length = rules.get('min_length')
                    max_length = rules.get('max_length')
                    if min_length is not None or max_length is not None:
                        length_condition = None
                        if min_length is not None:
                            length_condition = length(col(column)) >= min_length
                        if max_length is not None:
                            max_condition = length(col(column)) <= max_length
                            length_condition = (length_condition & max_condition) if length_condition else max_condition
                        
                        if length_condition:
                            df_cleaned = df_cleaned.filter(length_condition)
                    
                    # Apply pattern validation
                    pattern = rules.get('pattern')
                    if pattern:
                        df_cleaned = df_cleaned.filter(col(column).rlike(pattern))
                    
                    # Apply custom transformations
                    transformations = rules.get('transformations', [])
                    for transform in transformations:
                        transform_type = transform.get('type')
                        if transform_type == 'replace':
                            df_cleaned = df_cleaned.withColumn(
                                column,
                                regexp_replace(
                                    col(column),
                                    transform['pattern'],
                                    transform['replacement']
                                )
                            )
                        elif transform_type == 'extract':
                            df_cleaned = df_cleaned.withColumn(
                                column,
                                regexp_extract(col(column), transform['pattern'], 0)
                            )
                        elif transform_type == 'split':
                            df_cleaned = df_cleaned.withColumn(
                                column,
                                element_at(split(col(column), transform['delimiter']), transform['index'])
                            )
                    
                    # Apply profanity filtering if enabled
                    if rules.get('filter_profanity', False):
                        profanity_list = rules.get('profanity_list', [])
                        if profanity_list:
                            profanity_pattern = '|'.join(profanity_list)
                            df_cleaned = df_cleaned.withColumn(
                                column,
                                regexp_replace(col(column), profanity_pattern, '***')
                            )
                    
                    # Apply data masking if required
                    if rules.get('mask_sensitive_data', False):
                        mask_pattern = rules.get('mask_pattern', r'\d{4}')
                        df_cleaned = df_cleaned.withColumn(
                            column,
                            regexp_replace(col(column), mask_pattern, '****')
                        )
                    
                    # Calculate statistics
                    final_count = df_cleaned.count()
                    column_stats['rows_affected'] = original_count - final_count
                    column_stats['cleaning_applied'] = rules.get('clean_text', True)
                    column_stats['case_normalization'] = case_rule
                    column_stats['length_validation'] = {
                        'min_length': min_length,
                        'max_length': max_length
                    }
                    column_stats['pattern_validation'] = bool(pattern)
                    column_stats['transformations_applied'] = len(transformations)
                    column_stats['profanity_filtered'] = rules.get('filter_profanity', False)
                    column_stats['data_masked'] = rules.get('mask_sensitive_data', False)
                    
                    stats[column] = column_stats
            
        except Exception as e:
            logger.error(f"Error in F8 - Validate Text Fields: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f9_ensure_relationships(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F9: Ensure Proper Data Relationships"""
        stats = {}
        
        try:
            relationship_rules = self.config.get('relationship_rules', [])
            df_cleaned = df
            relationship_stats = {}
            
            for rule in relationship_rules:
                rule_type = rule.get('type')
                
                if rule_type == 'foreign_key':
                    # Validate foreign key relationships
                    child_column = rule.get('child_column')
                    parent_table_path = rule.get('parent_table_path')
                    parent_column = rule.get('parent_column')
                    
                    if (child_column in df.columns and parent_table_path and parent_column):
                        try:
                            parent_df = self.spark.read.parquet(parent_table_path)
                            valid_parent_keys = parent_df.select(parent_column).distinct().rdd.flatMap(lambda x: x).collect()
                            
                            invalid_fk_count = df_cleaned.filter(
                                ~col(child_column).isin(valid_parent_keys)
                            ).count()
                            
                            df_cleaned = df_cleaned.filter(col(child_column).isin(valid_parent_keys))
                            
                            relationship_stats[f"{child_column}_fk"] = {
                                'invalid_references_removed': invalid_fk_count,
                                'valid_parent_keys_count': len(valid_parent_keys)
                            }
                            
                        except Exception as e:
                            logger.warning(f"Could not validate FK relationship: {str(e)}")
                
                elif rule_type == 'conditional':
                    # Validate conditional relationships (if A then B must be true)
                    condition_column = rule.get('condition_column')
                    condition_value = rule.get('condition_value')
                    dependent_column = rule.get('dependent_column')
                    dependent_required = rule.get('dependent_required', True)
                    
                    if (condition_column in df.columns and dependent_column in df.columns):
                        if dependent_required:
                            # If condition is met, dependent column must not be null
                            invalid_condition_count = df_cleaned.filter(
                                (col(condition_column) == condition_value) & 
                                col(dependent_column).isNull()
                            ).count()
                            
                            df_cleaned = df_cleaned.filter(
                                ~((col(condition_column) == condition_value) & 
                                  col(dependent_column).isNull())
                            )
                            
                            relationship_stats[f"{condition_column}_{dependent_column}_conditional"] = {
                                'invalid_conditionals_removed': invalid_condition_count
                            }
            
            stats['relationship_stats'] = relationship_stats
            
        except Exception as e:
            logger.error(f"Error in F9 - Ensure Relationships: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f10_implement_entry_rules(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """F10: Implement Data Entry Rules"""
        stats = {}
        
        try:
            entry_rules = self.config.get('entry_rules', [])
            df_cleaned = df
            rule_stats = {}
            
            for rule in entry_rules:
                rule_name = rule.get('name', 'unnamed_rule')
                rule_type = rule.get('type')
                
                if rule_type == 'range_check':
                    column = rule.get('column')
                    min_value = rule.get('min_value')
                    max_value = rule.get('max_value')
                    
                    if column in df.columns:
                        invalid_range_count = df_cleaned.filter(
                            (col(column) < min_value) | (col(column) > max_value)
                        ).count()
                        
                        df_cleaned = df_cleaned.filter(
                            (col(column) >= min_value) & (col(column) <= max_value)
                        )
                        
                        rule_stats[rule_name] = {
                            'type': 'range_check',
                            'invalid_records_removed': invalid_range_count
                        }
                
                elif rule_type == 'allowed_values':
                    column = rule.get('column')
                    allowed_values = rule.get('allowed_values', [])
                    
                    if column in df.columns and allowed_values:
                        invalid_values_count = df_cleaned.filter(
                            ~col(column).isin(allowed_values)
                        ).count()
                        
                        df_cleaned = df_cleaned.filter(col(column).isin(allowed_values))
                        
                        rule_stats[rule_name] = {
                            'type': 'allowed_values',
                            'invalid_records_removed': invalid_values_count,
                            'allowed_values_count': len(allowed_values)
                        }
                
                elif rule_type == 'cross_field_validation':
                    # Validate relationships between fields
                    field1 = rule.get('field1')
                    field2 = rule.get('field2')
                    operator = rule.get('operator', '>')  # '>', '<', '>=', '<=', '==', '!='
                    
                    if field1 in df.columns and field2 in df.columns:
                        if operator == '>':
                            condition = col(field1) > col(field2)
                        elif operator == '<':
                            condition = col(field1) < col(field2)
                        elif operator == '>=':
                            condition = col(field1) >= col(field2)
                        elif operator == '<=':
                            condition = col(field1) <= col(field2)
                        elif operator == '==':
                            condition = col(field1) == col(field2)
                        elif operator == '!=':
                            condition = col(field1) != col(field2)
                        else:
                            continue
                        
                        invalid_cross_field_count = df_cleaned.filter(~condition).count()
                        df_cleaned = df_cleaned.filter(condition)
                        
                        rule_stats[rule_name] = {
                            'type': 'cross_field_validation',
                            'invalid_records_removed': invalid_cross_field_count
                        }
            
            stats['rule_stats'] = rule_stats
            
        except Exception as e:
            logger.error(f"Error in F10 - Implement Entry Rules: {str(e)}")
            df_cleaned = df
            stats['error'] = str(e)
        
        return df_cleaned, stats
    
    def f11_filter_legal_domain(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """
        Filter content based on legal domain using BERTurk-Legal model
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            Tuple[DataFrame, Dict]: Processed DataFrame and statistics
        """
        try:
            legal_filter = LegalDomainFilter()
            text_column = self.config.get("text_column", "text")  # Default to 'text' column
            
            if text_column not in df.columns:
                raise ValidationError(f"Text column '{text_column}' not found in DataFrame")
            
            processed_df, stats = legal_filter.process(df, text_column)
            
            # Update metrics
            self.metrics.record_validation_stats("legal_domain_filtering", stats)
            
            return processed_df, stats
            
        except Exception as e:
            logger.error(f"Error in legal domain filtering: {str(e)}")
            raise ProcessingError(f"Legal domain filtering failed: {str(e)}")
    
    def _save_with_checkpoint(self, df: DataFrame, original_path: str, batch_index: int) -> str:
        """Save DataFrame with checkpoint and partitioning"""
        try:
            # Generate output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = os.path.basename(original_path).replace('.parquet', '')
            output_path = f"{self.output_dir}/cleaned_{filename}_{timestamp}_batch_{batch_index}.parquet"
            
            # Create checkpoint path
            checkpoint_path = f"{self.checkpoint_dir}/checkpoint_{filename}_{timestamp}_batch_{batch_index}"
            
            # Calculate optimal number of partitions
            num_partitions = max(1, df.count() // self.batch_size)
            
            # Repartition if needed
            if num_partitions > 1:
                df = df.repartition(num_partitions)
            
            # Write DataFrame with checkpoint and partitioning
            df.write \
                .mode('overwrite') \
                .option('checkpointLocation', checkpoint_path) \
                .option('maxRecordsPerFile', self.batch_size) \
                .option('compression', 'snappy') \
                .parquet(output_path)
            
            logger.info(f"Saved cleaned data to: {output_path}")
            logger.info(f"Checkpoint created at: {checkpoint_path}")
            logger.info(f"Used {num_partitions} partitions with batch size {self.batch_size}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Error saving DataFrame: {str(e)}")
            raise ProcessingError(f"Failed to save DataFrame: {str(e)}")
    
    def _generate_summary_statistics(self, results: Dict) -> Dict:
        """Generate summary statistics from processing results"""
        summary = {
            'total_files_processed': len(results['processed_files']),
            'total_files_failed': len(results['failed_files']),
            'total_records_processed': results['total_records_processed'],
            'total_records_cleaned': results['total_records_cleaned'],
            'data_quality_improvement': 0.0,
            'processing_timestamp': datetime.now().isoformat()
        }
        
        if results['total_records_processed'] > 0:
            summary['data_quality_improvement'] = (
                (results['total_records_cleaned'] / results['total_records_processed']) * 100
            )
        
        return summary
    
    def _save_processing_results(self, results: Dict):
        """Save processing results to JSON file"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            results_path = f"{self.output_dir}/processing_results_{timestamp}.json"
            
            # Add metrics to results
            results['metrics'] = self.metrics.get_metrics()
            results['metrics_summary'] = self.metrics.get_summary()
            
            with open(results_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"Processing results saved to: {results_path}")
            
        except Exception as e:
            logger.error(f"Error saving processing results: {str(e)}")

def create_sample_config() -> Dict[str, Any]:
    """Create a sample configuration for the data quality framework"""
    return {
        'checkpoint_dir': '/tmp/dq_checkpoints',
        'output_dir': '/tmp/dq_output',
        'batch_size': 1000000,
        'missing_value_strategy': 'fill',
        'missing_threshold': 10.0,
        'critical_columns': ['id', 'name', 'value'],
        'fill_values': {
            'name': 'unknown',
            'value': 0
        },
        'mandatory_fields': ['id', 'name'],
        'numerical_columns': ['value'],
        'decimal_places': 2,
        'date_columns': ['created_at', 'updated_at'],
        'data_retention_days': 90,
        'irrelevant_data_conditions': [
            {
                'column': 'status',
                'values': ['deleted', 'archived']
            }
        ],
        'reference_data': [
            {
                'path': '/path/to/reference/data.parquet',
                'key_column': 'id',
                'df_column': 'reference_id'
            }
        ],
        'unique_constraints': [
            {
                'columns': ['id'],
                'action': 'keep_latest',
                'order_by': 'updated_at'
            }
        ],
        'category_mappings': {
            'status': {
                'active': 'A',
                'inactive': 'I',
                'pending': 'P'
            }
        },
        'text_standardization': {
            'name': {
                'trim': True,
                'case': 'title'
            }
        },
        'text_validation_rules': {
            'name': {
                'min_length': 2,
                'max_length': 100,
                'remove_special_chars': True,
                'clean_text': True,
                'case_normalization': 'title'
            }
        },
        'text_column': 'text',
        'legal_domain_filtering': {
            'enabled': True,
            'threshold': 0.8
        }
    }

class TestDataQualityFramework(unittest.TestCase):
    """Test suite for the Data Quality Framework"""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment"""
        cls.spark = SparkSession.builder \
            .appName("DataQualityFrameworkTests") \
            .master("local[2]") \
            .getOrCreate()
        
        # Create temporary directories for testing
        cls.test_dir = tempfile.mkdtemp()
        cls.checkpoint_dir = f"{cls.test_dir}/checkpoints"
        cls.output_dir = f"{cls.test_dir}/output"
        os.makedirs(cls.checkpoint_dir)
        os.makedirs(cls.output_dir)
        
        # Create test configuration
        cls.test_config = {
            'checkpoint_dir': cls.checkpoint_dir,
            'output_dir': cls.output_dir,
            'batch_size': 1000,
            'missing_value_strategy': 'drop',
            'mandatory_fields': ['id', 'value'],
            'numerical_columns': ['value'],
            'date_columns': ['date'],
            'data_retention_days': 30
        }
        
        # Initialize framework
        cls.framework = DataQualityFramework(cls.spark, cls.test_config)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment"""
        cls.spark.stop()
        shutil.rmtree(cls.test_dir)
    
    def setUp(self):
        """Set up test data before each test"""
        # Create test DataFrame
        self.test_data = [
            (1, "value1", 100.0, "2023-01-01"),
            (2, None, 200.0, "2023-01-02"),
            (3, "value3", None, "2023-01-03"),
            (4, "value4", 400.0, None)
        ]
        self.test_df = self.spark.createDataFrame(
            self.test_data,
            ["id", "value", "amount", "date"]
        )
    
    def test_f1_check_missing_values(self):
        """Test missing value check functionality"""
        df_cleaned, stats = self.framework.f1_check_missing_values(self.test_df)
        
        # Verify results
        self.assertEqual(df_cleaned.count(), 1)  # Only row with id=1 should remain
        self.assertIn('value', stats)
        self.assertIn('amount', stats)
        self.assertIn('date', stats)
    
    def test_f2_ensure_mandatory_fields(self):
        """Test mandatory fields validation"""
        df_cleaned, stats = self.framework.f2_ensure_mandatory_fields(self.test_df)
        
        # Verify results
        self.assertEqual(df_cleaned.count(), 2)  # Rows with id=1 and id=4 should remain
        self.assertIn('original_count', stats)
        self.assertIn('final_count', stats)
    
    def test_f3_standardize_numerical_formats(self):
        """Test numerical format standardization"""
        df_cleaned, stats = self.framework.f3_standardize_numerical_formats(self.test_df)
        
        # Verify results
        self.assertEqual(df_cleaned.count(), 4)  # All rows should remain
        self.assertIn('standardized_columns', stats)
    
    def test_f4_remove_outdated_data(self):
        """Test outdated data removal"""
        df_cleaned, stats = self.framework.f4_remove_outdated_data(self.test_df)
        
        # Verify results
        self.assertIn('original_count', stats)
        self.assertIn('final_count', stats)
    
    def test_process_parquet_files(self):
        """Test the main processing pipeline"""
        # Create test parquet file
        test_parquet_path = f"{self.test_dir}/test.parquet"
        self.test_df.write.parquet(test_parquet_path)
        
        # Process the file
        results = self.framework.process_parquet_files([test_parquet_path])
        
        # Verify results
        self.assertIn('processed_files', results)
        self.assertIn('failed_files', results)
        self.assertIn('validation_summary', results)
        self.assertEqual(len(results['processed_files']), 1)
    
    def test_metrics_collection(self):
        """Test metrics collection functionality"""
        # Process test data
        df_cleaned, _ = self.framework.f1_check_missing_values(self.test_df)
        
        # Get metrics
        metrics = self.framework.metrics.get_metrics()
        summary = self.framework.metrics.get_summary()
        
        # Verify metrics
        self.assertIn('processing_times', metrics)
        self.assertIn('memory_usage', metrics)
        self.assertIn('record_counts', metrics)
        self.assertIn('validation_stats', metrics)
        
        # Verify summary
        self.assertIn('total_processing_time', summary)
        self.assertIn('average_processing_times', summary)
        self.assertIn('peak_memory_usage', summary)
        self.assertIn('total_records_processed', summary)

if __name__ == "__main__":
    # Run tests
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
    
    # Example usage
    from pyspark.sql import SparkSession
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Data Quality Framework") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Create sample configuration
    config = create_sample_config()
    
    # Initialize framework
    dq_framework = DataQualityFramework(spark, config)
    
    # List of parquet files to process
    input_files = [
        "/path/to/your/data1.parquet",
        "/path/to/your/data2.parquet",
        "/path/to/your/data3.parquet"
    ]
    
    # Process files
    results = dq_framework.process_parquet_files(input_files)
    
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
    
    # Close Spark session
    spark.stop() 