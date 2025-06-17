#!/usr/bin/env python3
"""
Batch Processor for Data Quality Framework
Handles efficient processing of large Parquet datasets in batches
"""

import logging
import os
import time
from typing import Dict, List, Optional, Iterator, Tuple, Any, Callable
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import uuid
import psutil

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, spark_partition_id, monotonically_increasing_id
from pyspark.storagelevel import StorageLevel

from data_quality_framework import DataQualityError, ProcessingError
from src.logging_manager import LoggingManager

class BatchProcessingError(DataQualityError):
    """Raised when batch processing fails"""
    pass

class BatchProcessor:
    """
    Efficient batch processor for large Parquet datasets
    """
    
    def __init__(self, spark_session: SparkSession, batch_config: Dict[str, Any]):
        """
        Initialize Batch Processor
        
        Args:
            spark_session: Active Spark session
            batch_config: Configuration for batch processing
        """
        self.spark = spark_session
        self.config = batch_config
        self.batch_size = batch_config.get('batch_size', 1000000)
        self.max_concurrent_batches = batch_config.get('max_concurrent_batches', 3)
        self.enable_checkpointing = batch_config.get('enable_checkpointing', True)
        self.checkpoint_frequency = batch_config.get('checkpoint_frequency', 5)
        self.memory_optimization = batch_config.get('memory_optimization', True)
        self.checkpoint_dir = batch_config.get('checkpoint_dir', '/tmp/dq_checkpoints')
        self.max_checkpoint_age_days = batch_config.get('max_checkpoint_age_days', 7)
        
        # Initialize logging manager
        self.logger = LoggingManager(
            log_dir=batch_config.get('log_dir', 'logs'),
            log_level=logging.INFO,
            max_log_size=batch_config.get('max_log_size', 100 * 1024 * 1024),
            backup_count=batch_config.get('log_backup_count', 5)
        )
        
        # Performance monitoring
        self.batch_stats = {
            'total_batches': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'total_processing_time': 0.0,
            'average_batch_time': 0.0,
            'memory_usage': []
        }
        
        # Create checkpoint directory if it doesn't exist
        os.makedirs(self.checkpoint_dir, exist_ok=True)
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup resources"""
        self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        try:
            # Unpersist any cached DataFrames
            if hasattr(self, 'df_optimized'):
                self.df_optimized.unpersist()
            
            # Clean up old checkpoints
            self._cleanup_old_checkpoints()
            
            # Cleanup logging resources
            self.logger.cleanup()
            
            self.logger.logger.info("Batch processor resources cleaned up successfully")
        except Exception as e:
            self.logger.log_error(e, {"context": "cleanup"})
    
    def _cleanup_old_checkpoints(self):
        """Clean up checkpoints older than max_age_days"""
        try:
            current_time = datetime.now()
            for checkpoint in os.listdir(self.checkpoint_dir):
                checkpoint_path = os.path.join(self.checkpoint_dir, checkpoint)
                if os.path.getmtime(checkpoint_path) < (current_time - timedelta(days=self.max_checkpoint_age_days)).timestamp():
                    os.remove(checkpoint_path)
                    self.logger.logger.info(f"Removed old checkpoint: {checkpoint}")
        except Exception as e:
            self.logger.log_error(e, {"context": "checkpoint_cleanup"})
    
    def process_large_dataset(self, 
                            df: DataFrame, 
                            processing_function: Callable[[DataFrame], DataFrame],
                            output_path: str,
                            partition_column: Optional[str] = None) -> Dict[str, Any]:
        """
        Process large dataset in batches
        
        Args:
            df: Input DataFrame
            processing_function: Function to apply to each batch
            output_path: Path to save processed data
            partition_column: Column to use for partitioning (optional)
            
        Returns:
            Dictionary with processing statistics
        """
        self.logger.start_operation("process_large_dataset")
        
        try:
            # Log initial dataset stats
            total_rows = df.count()
            self.logger.logger.info(f"Starting batch processing of dataset with {total_rows:,} rows")
            
            # Optimize DataFrame for batch processing
            self.df_optimized = self._optimize_dataframe_for_batching(df, partition_column)
            
            # Create batches
            batches = self._create_batches(self.df_optimized, partition_column)
            total_batches = len(batches)
            self.logger.logger.info(f"Created {total_batches} batches for processing")
            
            # Process batches
            results = self._process_batches(batches, processing_function, output_path)
            
            # Merge results if needed
            if self.config.get('merge_results', True):
                self._merge_batch_results(output_path, results)
            
            # Log final statistics
            summary = self._generate_processing_summary(results)
            self.logger.log_accuracy_metrics({
                "success_rate": summary['success_rate'],
                "average_processing_time": summary['average_processing_time'],
                "total_processed_rows": summary['total_processed_rows']
            })
            
            self.logger.end_operation("process_large_dataset", 
                                    accuracy=summary['success_rate'],
                                    additional_metrics=summary)
            
            return summary
            
        except Exception as e:
            self.logger.log_error(e, {
                "context": "process_large_dataset",
                "total_rows": total_rows,
                "partition_column": partition_column
            })
            raise BatchProcessingError(f"Failed to process dataset: {str(e)}")
    
    def _optimize_dataframe_for_batching(self, 
                                       df: DataFrame, 
                                       partition_column: Optional[str] = None) -> DataFrame:
        """Optimize DataFrame for efficient batch processing"""
        try:
            # Add batch ID column for tracking
            df_with_batch_id = df.withColumn("batch_id", monotonically_increasing_id())
            
            # Repartition if specified
            if partition_column and partition_column in df.columns:
                df_optimized = df_with_batch_id.repartition(col(partition_column))
            else:
                # Calculate optimal number of partitions
                total_rows = df.count()
                optimal_partitions = max(1, min(200, total_rows // self.batch_size))
                df_optimized = df_with_batch_id.repartition(optimal_partitions)
            
            # Cache for better performance if memory optimization is enabled
            if self.memory_optimization:
                df_optimized = df_optimized.persist(StorageLevel.MEMORY_AND_DISK)
            
            return df_optimized
            
        except Exception as e:
            raise BatchProcessingError(f"Failed to optimize DataFrame: {str(e)}")
    
    def _process_single_batch(self, 
                            batch: Dict[str, Any], 
                            processing_function: Callable[[DataFrame], DataFrame],
                            output_path: str) -> Dict[str, Any]:
        """Process a single batch"""
        batch_id = batch['batch_id']
        self.logger.start_operation(f"process_batch_{batch_id}")
        
        try:
            # Process batch
            start_time = time.time()
            processed_df = processing_function(batch['dataframe'])
            processing_time = time.time() - start_time
            
            # Save batch result
            batch_output_path = self._save_batch_result(processed_df, output_path)
            
            # Create checkpoint if enabled
            if self.enable_checkpointing and batch_id % self.checkpoint_frequency == 0:
                self._create_checkpoint(batch_id, output_path)
            
            # Record memory usage
            memory_usage = psutil.Process().memory_info().rss / 1024 / 1024  # MB
            self.batch_stats['memory_usage'].append(memory_usage)
            
            # Log batch completion
            self.logger.logger.info(f"Processing batch {batch_id} with {batch['row_count']} rows")
            self.logger.logger.info(f"Batch {batch_id} processed successfully")
            
            return {
                'batch_id': batch_id,
                'status': 'success',
                'output_path': batch_output_path,
                'row_count': batch['row_count'],
                'processing_time': processing_time,
                'memory_usage_mb': memory_usage
            }
            
        except Exception as e:
            self.logger.log_error(e, {
                "context": f"process_batch_{batch_id}",
                "batch_id": batch_id
            })
            return {
                'batch_id': batch_id,
                'status': 'failed',
                'error': str(e)
            }
    
    def _process_batches(self, 
                        batches: List[Dict[str, Any]], 
                        processing_function: Callable[[DataFrame], DataFrame],
                        output_path: str) -> List[Dict[str, Any]]:
        """Process batches concurrently or sequentially"""
        results = []
        
        try:
            if self.max_concurrent_batches > 1:
                # Concurrent processing
                with ThreadPoolExecutor(max_workers=self.max_concurrent_batches) as executor:
                    future_to_batch = {
                        executor.submit(
                            self._process_single_batch, 
                            batch, 
                            processing_function, 
                            output_path
                        ): batch for batch in batches
                    }
                    
                    for future in as_completed(future_to_batch):
                        batch = future_to_batch[future]
                        try:
                            result = future.result()
                            results.append(result)
                            self.batch_stats['total_batches'] += 1
                            if result['status'] == 'success':
                                self.batch_stats['successful_batches'] += 1
                            else:
                                self.batch_stats['failed_batches'] += 1
                        except Exception as e:
                            self.logger.logger.error(f"Error processing batch {batch['batch_id']}: {str(e)}")
                            results.append({
                                'batch_id': batch['batch_id'],
                                'status': 'failed',
                                'error': str(e)
                            })
                            self.batch_stats['failed_batches'] += 1
            else:
                # Sequential processing
                for batch in batches:
                    result = self._process_single_batch(batch, processing_function, output_path)
                    results.append(result)
                    self.batch_stats['total_batches'] += 1
                    if result['status'] == 'success':
                        self.batch_stats['successful_batches'] += 1
                    else:
                        self.batch_stats['failed_batches'] += 1
            
            return results
            
        except Exception as e:
            raise BatchProcessingError(f"Failed to process batches: {str(e)}")
    
    def _generate_processing_summary(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Generate summary of batch processing results"""
        return {
            'total_batches': self.batch_stats['total_batches'],
            'successful_batches': self.batch_stats['successful_batches'],
            'failed_batches': self.batch_stats['failed_batches'],
            'total_processing_time': self.batch_stats['total_processing_time'],
            'average_batch_time': self.batch_stats['average_batch_time'],
            'average_memory_usage': sum(self.batch_stats['memory_usage']) / len(self.batch_stats['memory_usage']) if self.batch_stats['memory_usage'] else 0,
            'max_memory_usage': max(self.batch_stats['memory_usage']) if self.batch_stats['memory_usage'] else 0,
            'processing_timestamp': datetime.now().isoformat()
        }

    def _create_batches(self, df: DataFrame, partition_column: Optional[str] = None) -> List[Dict[str, Any]]:
        """Create batches from DataFrame"""
        try:
            # Get total number of rows
            total_rows = df.count()
            
            # Calculate number of batches
            num_batches = max(1, total_rows // self.batch_size)
            
            # Create batch definitions
            batches = []
            for i in range(num_batches):
                start_idx = i * self.batch_size
                end_idx = min((i + 1) * self.batch_size, total_rows)
                
                batch = {
                    'batch_id': i,
                    'start_idx': start_idx,
                    'end_idx': end_idx,
                    'row_count': end_idx - start_idx
                }
                batches.append(batch)
            
            return batches
            
        except Exception as e:
            raise BatchProcessingError(f"Failed to create batches: {str(e)}")
    
    def _merge_batch_results(self, output_path: str, results: List[Dict[str, Any]]) -> None:
        """Merge results from all batches"""
        try:
            # Read all batch results
            batch_dfs = []
            for result in results:
                if result['status'] == 'success':
                    batch_path = result['output_path']
                    batch_df = self.spark.read.parquet(batch_path)
                    batch_dfs.append(batch_df)
            
            if not batch_dfs:
                raise BatchProcessingError("No successful batch results to merge")
            
            # Union all DataFrames
            merged_df = batch_dfs[0]
            for df in batch_dfs[1:]:
                merged_df = merged_df.union(df)
            
            # Save merged result
            merged_df.write.mode('overwrite').parquet(output_path)
            
            # Clean up individual batch files
            if self.config.get('cleanup_batch_files', True):
                for result in results:
                    if result['status'] == 'success':
                        os.remove(result['output_path'])
            
        except Exception as e:
            raise BatchProcessingError(f"Failed to merge batch results: {str(e)}")
    
    def _save_batch_result(self, df: DataFrame, output_path: str) -> None:
        """Save batch result to disk"""
        try:
            df.write.mode('overwrite').parquet(output_path)
        except Exception as e:
            raise BatchProcessingError(f"Failed to save batch result: {str(e)}")
    
    def _create_checkpoint(self, batch_id: int, output_path: str) -> str:
        """Create checkpoint for batch"""
        try:
            checkpoint_path = os.path.join(self.checkpoint_dir, f"batch_{batch_id}_checkpoint")
            os.makedirs(checkpoint_path, exist_ok=True)
            return checkpoint_path
        except Exception as e:
            raise BatchProcessingError(f"Failed to create checkpoint: {str(e)}")

    def sample_processing_function(self, batch_df: DataFrame) -> DataFrame:
        """Sample processing function that returns only the DataFrame"""
        # Process the batch
        processed_df = batch_df.filter(col("value") > 0) \
                              .withColumn("processed_value", col("value") * 2)
        
        # Return only the processed DataFrame
        return processed_df


class BatchOptimizer:
    """Optimizes batch processing parameters based on data characteristics"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = LoggingManager(
            log_dir='logs',
            log_level=logging.INFO,
            max_log_size=100 * 1024 * 1024,
            backup_count=5
        )
    
    def analyze_dataset(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze dataset to recommend optimal batch parameters"""
        
        analysis = {}
        
        try:
            # Basic statistics
            total_rows = df.count()
            num_partitions = df.rdd.getNumPartitions()
            num_columns = len(df.columns)
            
            # Estimate memory usage
            sample_df = df.sample(0.01, seed=42)  # 1% sample
            sample_size = sample_df.count()
            
            if sample_size > 0:
                # Rough estimate of row size in bytes
                estimated_row_size = 100  # Default estimate
                estimated_total_size = total_rows * estimated_row_size
            else:
                estimated_total_size = 0
            
            analysis = {
                'total_rows': total_rows,
                'num_partitions': num_partitions,
                'num_columns': num_columns,
                'estimated_size_mb': estimated_total_size / (1024 * 1024),
                'avg_rows_per_partition': total_rows / num_partitions if num_partitions > 0 else 0
            }
            
            # Recommendations
            analysis['recommendations'] = self._generate_recommendations(analysis)
            
        except Exception as e:
            self.logger.logger.error(f"Error analyzing dataset: {str(e)}")
            analysis['error'] = str(e)
        
        return analysis
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate batch processing recommendations"""
        
        recommendations = {}
        
        total_rows = analysis['total_rows']
        estimated_size_mb = analysis['estimated_size_mb']
        
        # Recommend batch size
        if total_rows < 100000:
            recommended_batch_size = total_rows  # Process all at once
        elif total_rows < 1000000:
            recommended_batch_size = 100000
        elif total_rows < 10000000:
            recommended_batch_size = 500000
        else:
            recommended_batch_size = 1000000
        
        # Recommend number of concurrent batches
        if estimated_size_mb < 1000:  # < 1GB
            recommended_concurrency = 2
        elif estimated_size_mb < 5000:  # < 5GB
            recommended_concurrency = 3
        else:
            recommended_concurrency = 4
        
        # Recommend checkpointing frequency
        total_batches = (total_rows + recommended_batch_size - 1) // recommended_batch_size
        if total_batches <= 5:
            checkpoint_frequency = max(1, total_batches // 2)
        else:
            checkpoint_frequency = 5
        
        recommendations = {
            'batch_size': recommended_batch_size,
            'max_concurrent_batches': recommended_concurrency,
            'checkpoint_frequency': checkpoint_frequency,
            'enable_memory_optimization': estimated_size_mb > 1000,
            'enable_checkpointing': total_batches > 3,
            'estimated_processing_time_minutes': max(1, total_batches * 2)  # Rough estimate
        }
        
        return recommendations


if __name__ == "__main__":
    # Example usage
    from pyspark.sql import SparkSession
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Batch Processor Example") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    # Sample batch configuration
    batch_config = {
        'batch_size': 500000,
        'max_concurrent_batches': 2,
        'enable_checkpointing': True,
        'checkpoint_frequency': 3,
        'memory_optimization': True,
        'merge_results': True,
        'cleanup_batch_files': False
    }
    
    # Create sample DataFrame (replace with your actual data loading)
    sample_data = [(i, f"name_{i}", i * 10.5) for i in range(1000000)]
    df = spark.createDataFrame(sample_data, ["id", "name", "value"])
    
    # Analyze dataset for optimization recommendations
    optimizer = BatchOptimizer(spark)
    analysis = optimizer.analyze_dataset(df)
    
    print("Dataset Analysis:")
    for key, value in analysis.items():
        print(f"  {key}: {value}")
    
    # Create batch processor
    processor = BatchProcessor(spark, batch_config)
    
    # Define a sample processing function
    def sample_processing_function(batch_df):
        """Sample processing function for demonstration"""
        # Apply some transformations
        processed_df = batch_df.filter(col("value") > 0) \
                              .withColumn("processed_value", col("value") * 2)
        
        # Return only the processed DataFrame
        return processed_df
    
    # Process dataset in batches
    output_path = "/tmp/batch_processing_output"
    results = processor.process_large_dataset(
        df=df,
        processing_function=sample_processing_function,
        output_path=output_path
    )
    
    # Print results
    print("\nBatch Processing Results:")
    for key, value in results.items():
        print(f"  {key}: {value}")
    
    spark.stop() 