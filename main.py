#!/usr/bin/env python3
"""
Main execution point for the PySpark Data Quality Framework
Orchestrates the complete data quality pipeline
"""

import argparse
import logging
import sys
import time
from typing import List, Dict, Any, Optional
from pathlib import Path

# Import our custom modules
from data_quality_framework import DataQualityFramework
from config_manager import ConfigurationManager, create_sample_configurations
from batch_processor import BatchProcessor, BatchOptimizer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def setup_logging(log_level: str = "INFO", log_file: str = "data_quality_pipeline.log"):
    """Setup logging configuration"""
    
    log_levels = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    
    level = log_levels.get(log_level.upper(), logging.INFO)
    
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set Spark logging to WARN to reduce noise
    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("pyspark").setLevel(logging.WARN)


def create_spark_session(app_name: str = "Data Quality Framework") -> SparkSession:
    """Create optimized Spark session"""
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
        .getOrCreate()
    
    # Set log level for Spark
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def validate_input_paths(input_paths: List[str]) -> List[str]:
    """Validate that input paths exist and are accessible"""
    
    valid_paths = []
    invalid_paths = []
    
    for path in input_paths:
        try:
            # For local filesystem, check if path exists
            if path.startswith('file://') or not path.startswith(('s3://', 'hdfs://', 'gs://')):
                local_path = path.replace('file://', '')
                if Path(local_path).exists():
                    valid_paths.append(path)
                else:
                    invalid_paths.append(path)
            else:
                # For distributed filesystems, assume valid (Spark will handle errors)
                valid_paths.append(path)
                
        except Exception as e:
            logging.warning(f"Could not validate path {path}: {str(e)}")
            invalid_paths.append(path)
    
    if invalid_paths:
        logging.warning(f"Invalid paths found: {invalid_paths}")
    
    return valid_paths


def run_data_quality_pipeline(input_paths: List[str], 
                             config_path: Optional[str] = None,
                             output_dir: Optional[str] = None,
                             batch_processing: bool = True,
                             analyze_only: bool = False) -> Dict[str, Any]:
    """
    Run the complete data quality pipeline
    
    Args:
        input_paths: List of paths to Parquet files
        config_path: Path to configuration file
        output_dir: Output directory for results
        batch_processing: Whether to use batch processing
        analyze_only: Only analyze data without processing
        
    Returns:
        Dictionary with pipeline results
    """
    
    logger = logging.getLogger(__name__)
    pipeline_start_time = time.time()
    
    # Initialize Spark session
    logger.info("Initializing Spark session...")
    spark = create_spark_session()
    
    try:
        # Load configuration
        logger.info("Loading configuration...")
        if config_path:
            config_manager = ConfigurationManager(config_path)
        else:
            logger.info("No config file provided, using sample configuration")
            config_manager = ConfigurationManager()
            sample_config = create_sample_configurations()
            config_manager._update_config_from_dict(sample_config)
        
        # Override output directory if provided
        if output_dir:
            config_manager.config.output_dir = output_dir
        
        # Validate configuration
        config_issues = config_manager.validate_config()
        if config_issues:
            logger.warning("Configuration validation issues found:")
            for issue in config_issues:
                logger.warning(f"  - {issue}")
        
        config_dict = config_manager.to_dict()
        
        # Validate input paths
        logger.info("Validating input paths...")
        valid_paths = validate_input_paths(input_paths)
        
        if not valid_paths:
            raise ValueError("No valid input paths found")
        
        logger.info(f"Processing {len(valid_paths)} valid paths")
        
        # Initialize framework components
        dq_framework = DataQualityFramework(spark, config_dict)
        
        pipeline_results = {
            'start_time': time.time(),
            'input_paths': valid_paths,
            'configuration_summary': config_manager.get_validation_summary(),
            'files_processed': [],
            'analysis_results': {},
            'processing_results': {},
            'total_files': len(valid_paths),
            'successful_files': 0,
            'failed_files': 0
        }
        
        if batch_processing:
            # Use batch processor for large datasets
            logger.info("Using batch processing mode...")
            
            batch_config = {
                'batch_size': config_dict.get('batch_size', 1000000),
                'max_concurrent_batches': 2,
                'enable_checkpointing': True,
                'checkpoint_frequency': 5,
                'memory_optimization': True,
                'merge_results': True,
                'cleanup_batch_files': False,
                'checkpoint_dir': config_dict.get('checkpoint_dir', '/tmp/dq_checkpoints'),
                'log_dir': config_dict.get('log_dir', 'logs'),
                'max_log_size': config_dict.get('max_log_size', 100 * 1024 * 1024),
                'log_backup_count': config_dict.get('log_backup_count', 5)
            }
            
            batch_processor = BatchProcessor(spark_session=spark, batch_config=batch_config)
            optimizer = BatchOptimizer(spark_session=spark)
            
            # Process each file with batch processing
            for i, file_path in enumerate(valid_paths):
                try:
                    logger.info(f"Processing file {i+1}/{len(valid_paths)}: {file_path}")
                    
                    # Load data
                    df = spark.read.parquet(file_path)
                    
                    # Analyze dataset
                    analysis = optimizer.analyze_dataset(df)
                    pipeline_results['analysis_results'][file_path] = analysis
                    
                    if analyze_only:
                        logger.info(f"Analysis complete for {file_path}")
                        continue
                    
                    # Define processing function that applies all data quality checks
                    def dq_processing_function(batch_df):
                        cleaned_df, validation_stats = dq_framework._apply_data_quality_pipeline(
                            batch_df, file_path
                        )
                        return cleaned_df  # Only return the DataFrame as expected by the batch processor
                    
                    # Process with batch processor
                    batch_output_path = f"{config_dict['output_dir']}/batch_processed_{Path(file_path).stem}"
                    batch_results = batch_processor.process_large_dataset(
                        df=df,
                        processing_function=dq_processing_function,
                        output_path=batch_output_path
                    )
                    
                    pipeline_results['processing_results'][file_path] = batch_results
                    pipeline_results['successful_files'] += 1
                    
                    logger.info(f"Successfully processed {file_path}")
                    
                except Exception as e:
                    logger.error(f"Failed to process {file_path}: {str(e)}")
                    pipeline_results['failed_files'] += 1
                    pipeline_results['processing_results'][file_path] = {
                        'status': 'failed',
                        'error': str(e)
                    }
        
        else:
            # Use standard processing for smaller datasets
            logger.info("Using standard processing mode...")
            
            if analyze_only:
                # Only perform analysis
                for file_path in valid_paths:
                    try:
                        df = spark.read.parquet(file_path)
                        optimizer = BatchOptimizer(spark_session=spark)  # type: ignore[reportCallIssue]
                        analysis = optimizer.analyze_dataset(df)
                        pipeline_results['analysis_results'][file_path] = analysis
                        logger.info(f"Analysis complete for {file_path}")
                    except Exception as e:
                        logger.error(f"Failed to analyze {file_path}: {str(e)}")
            else:
                # Standard data quality processing
                results = dq_framework.process_parquet_files(valid_paths)
                pipeline_results['processing_results'] = results
                pipeline_results['successful_files'] = len(results.get('processed_files', []))
                pipeline_results['failed_files'] = len(results.get('failed_files', []))
        
        # Calculate total processing time
        total_time = time.time() - pipeline_start_time
        pipeline_results['total_processing_time'] = total_time
        pipeline_results['end_time'] = time.time()
        
        # Generate final summary
        pipeline_results['summary'] = {
            'total_files': pipeline_results['total_files'],
            'successful_files': pipeline_results['successful_files'],
            'failed_files': pipeline_results['failed_files'],
            'success_rate': (pipeline_results['successful_files'] / pipeline_results['total_files']) * 100,
            'total_processing_time_minutes': total_time / 60,
            'processing_mode': 'batch' if batch_processing else 'standard',
            'analyze_only': analyze_only
        }
        
        # Save pipeline results
        save_pipeline_results(pipeline_results, config_dict['output_dir'])
        
        logger.info("Pipeline execution completed successfully")
        return pipeline_results
        
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise
        
    finally:
        # Clean up Spark session
        spark.stop()


def save_pipeline_results(results: Dict[str, Any], output_dir: str):
    """Save pipeline results to JSON file"""
    
    import json
    import os
    from datetime import datetime
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results_file = f"{output_dir}/pipeline_results_{timestamp}.json"
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logging.info(f"Pipeline results saved to: {results_file}")
        
    except Exception as e:
        logging.error(f"Failed to save pipeline results: {str(e)}")


def print_pipeline_summary(results: Dict[str, Any]):
    """Print a formatted summary of pipeline results"""
    
    print("\n" + "="*80)
    print("DATA QUALITY PIPELINE SUMMARY")
    print("="*80)
    
    summary = results.get('summary', {})
    
    print(f"Processing Mode: {summary.get('processing_mode', 'unknown').upper()}")
    print(f"Analysis Only: {summary.get('analyze_only', False)}")
    print(f"Total Files: {summary.get('total_files', 0)}")
    print(f"Successful Files: {summary.get('successful_files', 0)}")
    print(f"Failed Files: {summary.get('failed_files', 0)}")
    print(f"Success Rate: {summary.get('success_rate', 0):.2f}%")
    print(f"Total Processing Time: {summary.get('total_processing_time_minutes', 0):.2f} minutes")
    
    # Print analysis results if available
    if results.get('analysis_results'):
        print("\nDATASET ANALYSIS RESULTS:")
        print("-" * 40)
        for file_path, analysis in results['analysis_results'].items():
            print(f"\nFile: {Path(file_path).name}")
            print(f"  Total Rows: {analysis.get('total_rows', 0):,}")
            print(f"  Columns: {analysis.get('num_columns', 0)}")
            print(f"  Estimated Size: {analysis.get('estimated_size_mb', 0):.2f} MB")
            
            recommendations = analysis.get('recommendations', {})
            if recommendations:
                print(f"  Recommended Batch Size: {recommendations.get('batch_size', 0):,}")
                print(f"  Recommended Concurrency: {recommendations.get('max_concurrent_batches', 0)}")
    
    print("\n" + "="*80)


def main():
    """Main entry point"""
    
    parser = argparse.ArgumentParser(description="PySpark Data Quality Framework")
    
    parser.add_argument(
        'input_paths',
        nargs='+',
        help='Paths to Parquet files to process'
    )
    
    parser.add_argument(
        '--config',
        '-c',
        help='Path to configuration file (JSON or YAML)'
    )
    
    parser.add_argument(
        '--output-dir',
        '-o',
        help='Output directory for results'
    )
    
    parser.add_argument(
        '--batch-processing',
        action='store_true',
        default=True,
        help='Enable batch processing for large datasets'
    )
    
    parser.add_argument(
        '--no-batch-processing',
        action='store_true',
        help='Disable batch processing'
    )
    
    parser.add_argument(
        '--analyze-only',
        action='store_true',
        help='Only analyze data without processing'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Set logging level'
    )
    
    parser.add_argument(
        '--log-file',
        default='data_quality_pipeline.log',
        help='Log file path'
    )
    
    parser.add_argument(
        '--create-sample-configs',
        action='store_true',
        help='Create sample configuration files and exit'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level, args.log_file)
    logger = logging.getLogger(__name__)
    
    # Create sample configurations if requested
    if args.create_sample_configs:
        logger.info("Creating sample configuration files...")
        
        sample_configs = create_sample_configurations()
        for domain, config_data in sample_configs.items():
            config_manager = ConfigurationManager()
            config_manager._update_config_from_dict(config_data)
            
            config_manager.save_config(f"{domain}_data_quality_config.json", "json")
            config_manager.save_config(f"{domain}_data_quality_config.yaml", "yaml")
            
            logger.info(f"Created configuration files for {domain}")
        
        logger.info("Sample configuration files created successfully")
        return
    
    # Determine batch processing mode
    batch_processing = args.batch_processing and not args.no_batch_processing
    
    try:
        # Run the pipeline
        logger.info("Starting Data Quality Pipeline...")
        
        results = run_data_quality_pipeline(
            input_paths=args.input_paths,
            config_path=args.config,
            output_dir=args.output_dir,
            batch_processing=batch_processing,
            analyze_only=args.analyze_only
        )
        
        # Print summary
        print_pipeline_summary(results)
        
        logger.info("Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main() 