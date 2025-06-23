"""
Main Data Quality Framework class
"""

import logging
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import json
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
import pandas as pd

from .exceptions import ConfigurationError, ProcessingError
from ..processors import (
    MissingValuesProcessor,
    MandatoryFieldsProcessor,
    NumericalFormatsProcessor,
    OutdatedDataProcessor,
    ExternalValidationProcessor,
    UniquenessProcessor,
    CategoriesProcessor,
    TextValidationProcessor,
    RelationshipsProcessor,
    EntryRulesProcessor,
    TurkishDuplicateDetector
)
from ..processors.xlsx_processor import XLSXProcessor
from ..utils.metrics import MetricsCollector
from ..utils.config_validator import ConfigurationValidator
from ..utils.legal_domain_filter import LegalDomainFilter
from ..exceptions import ValidationError, ModelLoadError, InferenceError

logger = logging.getLogger(__name__)

def get_processor_registry(config):
    """
    Dynamically import and instantiate all processors, including TurkishDuplicateDetector.
    This avoids circular imports by only importing when needed.
    """
    return {
        'missing_values': MissingValuesProcessor(config),
        'mandatory_fields': MandatoryFieldsProcessor(config),
        'numerical_formats': NumericalFormatsProcessor(config),
        'outdated_data': OutdatedDataProcessor(config),
        'external_validation': ExternalValidationProcessor(config),
        'uniqueness': UniquenessProcessor(config),
        'categories': CategoriesProcessor(config),
        'text_validation': TextValidationProcessor(config),
        'relationships': RelationshipsProcessor(config),
        'entry_rules': EntryRulesProcessor(config),
        'turkish_duplicate_detector': TurkishDuplicateDetector(config)
    }

class DataQualityFramework:
    """
    Comprehensive data quality framework for PySpark DataFrame processing
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
        
        # Initialize processors using the helper function
        self.processors = get_processor_registry(config)
        
        self.xlsx_processor = XLSXProcessor(config)
        
        # Create directories if they don't exist
        self._create_directories()
    
    def _create_directories(self):
        """Create necessary directories for checkpoints and output"""
        try:
            os.makedirs(self.checkpoint_dir, exist_ok=True)
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Created directories: {self.checkpoint_dir}, {self.output_dir}")
        except Exception as e:
            logger.warning(f"Could not create local directories: {e}")
    
    def process_file(self, file_path: str) -> Dict:
        """
        Process a single file based on its type
        
        Args:
            file_path (str): Path to the file to process
            
        Returns:
            Dict: Processing results and metrics
        """
        file_path_obj = Path(file_path)
        
        if file_path_obj.suffix.lower() in self.xlsx_processor.supported_extensions:
            return self._process_xlsx_file(str(file_path_obj))
        else:
            raise ValueError(f"Unsupported file type: {file_path_obj.suffix}")
            
    def _process_xlsx_file(self, file_path: str) -> Dict:
        """
        Process an XLSX file
        
        Args:
            file_path (str): Path to the XLSX file
            
        Returns:
            Dict: Processing results and metrics
        """
        try:
            # Process the file
            df = self.xlsx_processor.process_file(file_path)
            
            # Generate output path
            output_dir = Path(self.config['output_dir'])
            output_path = output_dir / f"processed_{Path(file_path).name}"
            
            # Save processed file
            self.xlsx_processor.save_processed_file(df, str(output_path))
            
            # Calculate metrics
            metrics = {
                'total_rows': len(df),
                'duplicates_removed': len(pd.read_excel(file_path)) - len(df),
                'missing_values_filled': df.isna().sum().sum(),
                'processing_time': None  # TODO: Add timing
            }
            
            return {
                'status': 'success',
                'output_path': str(output_path),
                'metrics': metrics
            }
            
        except Exception as e:
            logger.error(f"Error processing XLSX file {file_path}: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }
            
    def process_files(self, file_paths: List[str]) -> Dict:
        """
        Process multiple files
        
        Args:
            file_paths (List[str]): List of file paths to process
            
        Returns:
            Dict: Summary of processing results
        """
        results = []
        total_files = len(file_paths)
        successful_files = 0
        
        for file_path in file_paths:
            result = self.process_file(file_path)
            results.append(result)
            if result['status'] == 'success':
                successful_files += 1
                
        return {
            'total_files': total_files,
            'successful_files': successful_files,
            'failed_files': total_files - successful_files,
            'results': results
        }
    
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
            # Apply each processor in sequence
            for processor_name, processor in self.processors.items():
                logger.info(f"Applying {processor_name} processor")
                start_time = self.metrics.start_timer(processor_name)
                
                current_df, stats = processor.process(current_df)
                
                self.metrics.end_timer(processor_name, start_time)
                self.metrics.record_memory_usage(processor_name)
                self.metrics.record_record_count(processor_name, current_df.count())
                self.metrics.record_validation_stats(processor_name, stats)
                
                validation_stats[processor_name] = stats
            
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
    
    def f11_filter_legal_domain(self, df: DataFrame) -> Tuple[DataFrame, Dict]:
        """
        Filter content based on legal domain using BERTurk-Legal model
        
        Args:
            df (DataFrame): Input DataFrame
            
        Returns:
            Tuple[DataFrame, Dict]: Processed DataFrame and statistics
            
        Raises:
            ValidationError: If required configuration is missing
            ProcessingError: If legal domain filtering fails
        """
        try:
            # Get legal domain filtering configuration
            legal_config = self.config.get('legal_domain_filtering', {})
            if not legal_config.get('enabled', False):
                logger.info("Legal domain filtering is disabled")
                return df, {'status': 'disabled'}
                
            # Initialize legal domain filter with configuration
            legal_filter = LegalDomainFilter(legal_config)
            
            # Process the data
            processed_df, stats = legal_filter.process(df, legal_config['text_column'])
            
            # Update metrics
            self.metrics.record_validation_stats("legal_domain_filtering", stats)
            
            # Log results
            logger.info(
                f"Legal domain filtering completed: {stats['legal_documents']} legal documents "
                f"({stats['legal_percentage']:.2f}%) out of {stats['total_documents']} total documents"
            )
            
            return processed_df, stats
            
        except ValidationError as e:
            logger.error(f"Configuration error in legal domain filtering: {str(e)}")
            raise
        except ModelLoadError as e:
            logger.error(f"Model loading error in legal domain filtering: {str(e)}")
            raise ProcessingError(f"Failed to load legal domain model: {str(e)}")
        except InferenceError as e:
            logger.error(f"Inference error in legal domain filtering: {str(e)}")
            raise ProcessingError(f"Legal domain model inference failed: {str(e)}")
        except Exception as e:
            logger.error(f"Unexpected error in legal domain filtering: {str(e)}")
            raise ProcessingError(f"Legal domain filtering failed: {str(e)}") 