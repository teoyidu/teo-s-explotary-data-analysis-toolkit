#!/usr/bin/env python3
"""
Example usage of the PySpark Data Quality Framework
Demonstrates how to create sample data and process it through the framework
"""

import logging
import os
import tempfile
from datetime import datetime, timedelta
import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rand
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Import our framework modules
from data_quality_framework import DataQualityFramework
from config_manager import ConfigurationManager, create_sample_configurations
from batch_processor import BatchProcessor, BatchOptimizer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_data(spark: SparkSession, num_records: int = 100000) -> None:
    """Create sample e-commerce data for demonstration"""
    
    logger.info(f"Creating sample data with {num_records} records...")
    
    # Define schema
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("discount", DoubleType(), True),
        StructField("customer_email", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("shipping_cost", DoubleType(), True)
    ])
    
    # Generate sample data
    sample_data = []
    base_date = datetime(2023, 1, 1)
    
    for i in range(num_records):
        # Introduce some data quality issues intentionally
        order_id = f"ORD-{i:06d}" if random.random() > 0.001 else None  # 0.1% missing
        customer_id = f"CUST-{random.randint(1, 10000):05d}"
        product_id = f"PROD-{random.randint(1, 1000):04d}"
        
        # Date range
        order_date = base_date + timedelta(days=random.randint(0, 365))
        
        quantity = random.randint(1, 10) if random.random() > 0.002 else None  # 0.2% missing
        price = round(random.uniform(10.0, 1000.0), 2)
        
        # Some negative prices (data quality issue)
        if random.random() < 0.001:
            price = -price
        
        discount = round(random.uniform(0.0, 100.0), 2) if random.random() > 0.05 else None  # 5% missing
        
        # Email with some invalid formats
        if random.random() > 0.01:  # 99% valid emails
            customer_email = f"customer{random.randint(1, 10000)}@email.com"
        else:  # 1% invalid emails
            customer_email = f"invalid_email_{i}"
        
        # Status with some inconsistent formats
        statuses = ["P", "S", "D", "C", "Pending", "Shipped"]  # Mixed formats
        order_status = random.choice(statuses)
        
        shipping_cost = round(random.uniform(0.0, 50.0), 2)
        
        sample_data.append((
            order_id, customer_id, product_id, order_date,
            quantity, price, discount, customer_email,
            order_status, shipping_cost
        ))
    
    # Create DataFrame
    df = spark.createDataFrame(sample_data, schema)
    
    # Add some duplicates (data quality issue)
    duplicate_fraction = 0.01  # 1% duplicates
    num_duplicates = int(num_records * duplicate_fraction)
    duplicates_df = df.limit(num_duplicates)
    df_with_duplicates = df.union(duplicates_df)
    
    # Save to Parquet
    output_path = tempfile.mkdtemp() + "/sample_ecommerce_data.parquet"
    df_with_duplicates.write.mode("overwrite").parquet(output_path)
    
    logger.info(f"Sample data saved to: {output_path}")
    logger.info(f"Total records (including duplicates): {df_with_duplicates.count()}")
    
    return output_path


def demonstrate_framework_usage():
    """Demonstrate the complete framework usage"""
    
    logger.info("Starting Data Quality Framework Demonstration")
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Data Quality Framework Demo") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    try:
        # Step 1: Create sample data
        sample_data_path = create_sample_data(spark, num_records=50000)
        
        # Step 2: Create configuration
        logger.info("Setting up configuration...")
        sample_configs = create_sample_configurations()
        ecommerce_config = sample_configs['ecommerce']
        
        # Modify for temporary directories
        temp_dir = tempfile.mkdtemp()
        ecommerce_config['checkpoint_dir'] = f"{temp_dir}/checkpoints"
        ecommerce_config['output_dir'] = f"{temp_dir}/output"
        
        config_manager = ConfigurationManager()
        config_manager.merge_config(ecommerce_config)
        
        # Validate configuration
        issues = config_manager.validate_config()
        if issues:
            logger.warning("Configuration issues:")
            for issue in issues:
                logger.warning(f"  - {issue}")
        else:
            logger.info("Configuration validation passed")
        
        # Step 3: Analyze dataset
        logger.info("Analyzing dataset...")
        df = spark.read.parquet(sample_data_path)
        
        optimizer = BatchOptimizer(spark)
        analysis = optimizer.analyze_dataset(df)
        
        logger.info("Dataset Analysis Results:")
        logger.info(f"  Total Rows: {analysis['total_rows']:,}")
        logger.info(f"  Columns: {analysis['num_columns']}")
        logger.info(f"  Estimated Size: {analysis['estimated_size_mb']:.2f} MB")
        
        recommendations = analysis.get('recommendations', {})
        if recommendations:
            logger.info("Recommendations:")
            logger.info(f"  Batch Size: {recommendations['batch_size']:,}")
            logger.info(f"  Concurrent Batches: {recommendations['max_concurrent_batches']}")
        
        # Step 4: Standard processing
        logger.info("Running standard data quality processing...")
        dq_framework = DataQualityFramework(spark, config_manager.to_dict())
        
        standard_results = dq_framework.process_parquet_files([sample_data_path])
        
        logger.info("Standard Processing Results:")
        logger.info(f"  Files Processed: {len(standard_results['processed_files'])}")
        logger.info(f"  Total Records Processed: {standard_results['total_records_processed']:,}")
        logger.info(f"  Total Records Cleaned: {standard_results['total_records_cleaned']:,}")
        logger.info(f"  Data Quality Improvement: {standard_results['validation_summary']['data_quality_improvement']:.2f}%")
        
        # Step 5: Batch processing demonstration
        logger.info("Running batch processing...")
        
        batch_config = {
            'batch_size': 10000,  # Smaller batches for demo
            'max_concurrent_batches': 2,
            'enable_checkpointing': True,
            'checkpoint_frequency': 2,
            'memory_optimization': True,
            'merge_results': True
        }
        
        batch_processor = BatchProcessor(spark, batch_config)
        
        # Define processing function
        def demo_processing_function(batch_df):
            """Demonstration processing function"""
            # Apply some of the data quality checks
            cleaned_df, validation_stats = dq_framework._apply_data_quality_pipeline(
                batch_df, "demo_batch"
            )
            return cleaned_df, validation_stats
        
        batch_output_path = f"{temp_dir}/batch_output"
        batch_results = batch_processor.process_large_dataset(
            df=df,
            processing_function=demo_processing_function,
            output_path=batch_output_path
        )
        
        logger.info("Batch Processing Results:")
        logger.info(f"  Total Batches: {batch_results['total_batches']}")
        logger.info(f"  Successful Batches: {batch_results['successful_batches']}")
        logger.info(f"  Success Rate: {batch_results['success_rate']:.2f}%")
        logger.info(f"  Processing Throughput: {batch_results['processing_throughput']:.0f} records/second")
        
        # Step 6: Demonstrate error handling
        logger.info("Demonstrating error handling...")
        
        # Create a problematic configuration
        bad_config = config_manager.to_dict().copy()
        bad_config['text_validation_rules']['email']['pattern'] = '[invalid_regex'  # Invalid regex
        
        try:
            bad_config_manager = ConfigurationManager()
            bad_config_manager.merge_config(bad_config)
            validation_issues = bad_config_manager.validate_config()
            if validation_issues:
                logger.info("Successfully caught configuration validation errors:")
                for issue in validation_issues:
                    logger.info(f"  - {issue}")
        except Exception as e:
            logger.info(f"Error handling demonstration: {str(e)}")
        
        # Step 7: Show final data quality metrics
        logger.info("\n" + "="*60)
        logger.info("FINAL DATA QUALITY METRICS")
        logger.info("="*60)
        
        # Load and analyze cleaned data
        if standard_results['processed_files']:
            cleaned_file_path = standard_results['processed_files'][0]['output_path']
            cleaned_df = spark.read.parquet(cleaned_file_path)
            
            original_count = df.count()
            cleaned_count = cleaned_df.count()
            
            logger.info(f"Original Records: {original_count:,}")
            logger.info(f"Cleaned Records: {cleaned_count:,}")
            logger.info(f"Records Removed: {original_count - cleaned_count:,}")
            logger.info(f"Data Quality Improvement: {(cleaned_count/original_count)*100:.2f}%")
            
            # Show validation statistics
            for file_result in standard_results['processed_files']:
                validation_stats = file_result.get('validation_stats', {})
                logger.info("\nValidation Steps Summary:")
                for step, stats in validation_stats.items():
                    if isinstance(stats, dict) and 'rows_dropped' in stats:
                        logger.info(f"  {step}: {stats['rows_dropped']} rows affected")
                    elif isinstance(stats, dict) and 'rows_removed' in stats:
                        logger.info(f"  {step}: {stats['rows_removed']} rows affected")
        
        logger.info("\nDemonstration completed successfully!")
        
        # Cleanup
        logger.info(f"Temporary files created in: {temp_dir}")
        logger.info("You can inspect the results in the temporary directory")
        
    except Exception as e:
        logger.error(f"Demonstration failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


def demonstrate_custom_validation():
    """Demonstrate adding custom validation functions"""
    
    logger.info("Demonstrating custom validation functions...")
    
    spark = SparkSession.builder \
        .appName("Custom Validation Demo") \
        .getOrCreate()
    
    try:
        # Create simple test data
        test_data = [
            ("1", "customer1@email.com", 25.50, "2023-01-01"),
            ("2", "invalid_email", -10.00, "2023-01-02"),  # Invalid email and negative price
            ("3", "customer3@email.com", 50.00, "2022-12-31"),  # Old date
            ("4", None, 75.25, "2023-01-03"),  # Missing ID
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("date", StringType(), True)
        ])
        
        df = spark.createDataFrame(test_data, schema)
        
        logger.info("Original data:")
        df.show()
        
        # Custom validation function
        def custom_email_validation(df_input):
            """Custom validation for email format"""
            valid_email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            
            # Count invalid emails
            invalid_count = df_input.filter(
                ~col("email").rlike(valid_email_pattern)
            ).count()
            
            # Filter valid emails
            df_cleaned = df_input.filter(col("email").rlike(valid_email_pattern))
            
            stats = {
                'invalid_emails_removed': invalid_count,
                'validation_type': 'custom_email'
            }
            
            return df_cleaned, stats
        
        # Apply custom validation
        cleaned_df, validation_stats = custom_email_validation(df)
        
        logger.info("After custom email validation:")
        cleaned_df.show()
        logger.info(f"Validation stats: {validation_stats}")
        
        # Chain multiple custom validations
        def custom_amount_validation(df_input):
            """Custom validation for positive amounts"""
            invalid_count = df_input.filter(col("amount") <= 0).count()
            df_cleaned = df_input.filter(col("amount") > 0)
            
            return df_cleaned, {'negative_amounts_removed': invalid_count}
        
        # Apply second validation
        final_df, amount_stats = custom_amount_validation(cleaned_df)
        
        logger.info("After amount validation:")
        final_df.show()
        logger.info(f"Amount validation stats: {amount_stats}")
        
        logger.info("Custom validation demonstration completed!")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    print("PySpark Data Quality Framework - Example Usage")
    print("=" * 50)
    
    # Run main demonstration
    demonstrate_framework_usage()
    
    print("\n" + "=" * 50)
    print("Custom Validation Demonstration")
    print("=" * 50)
    
    # Run custom validation demo
    demonstrate_custom_validation()
    
    print("\n" + "=" * 50)
    print("All demonstrations completed!")
    print("Check the log files and output directories for detailed results.") 