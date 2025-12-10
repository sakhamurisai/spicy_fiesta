"""
Utility functions for data insertion pipeline.

This module provides common functionality for creating Spark sessions
and writing Parquet files with consistent configuration.
"""
import os
import sys
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_spark(app_name: str, master: str = "local[*]") -> SparkSession:
    """
    Create and configure a Spark session.
    
    Args:
        app_name: Name for the Spark application
        master: Spark master URL (default: local[*])
        
    Returns:
        Configured SparkSession instance
        
    Example:
        >>> spark = get_spark("my_app")
        >>> spark.version
        '3.x.x'
    """
    try:
        # Ensure Spark Python worker uses the same Python executable as this process.
        python_exec = sys.executable
        os.environ.setdefault("PYSPARK_PYTHON", python_exec)

        builder = SparkSession.builder.appName(app_name).master(master)
        # Ensure executor and driver python configuration points to current python
        builder = builder.config("spark.pyspark.python", python_exec)
        builder = builder.config("spark.pyspark.driver.python", python_exec)
        builder = builder.config("spark.executorEnv.PYSPARK_PYTHON", python_exec)
        # Performance and parquet defaults
        builder = builder.config("spark.sql.adaptive.enabled", "true")
        builder = builder.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        builder = builder.config("spark.sql.parquet.compression.codec", "snappy")
        # Windows Hadoop compatibility - use algorithm v2 and ignore cleanup failures
        builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        builder = builder.config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        # Disable checksum filesystem to avoid native library issues on Windows
        builder = builder.config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        builder = builder.config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")

        spark = builder.getOrCreate()

        logger.info(f"Spark session created: {app_name} (python={python_exec})")
        return spark

    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partitionBy: Optional[str] = None
) -> None:
    """
    Write DataFrame to Parquet format with consistent configuration.
    
    Args:
        df: DataFrame to write
        path: Output path for Parquet files
        mode: Write mode (overwrite, append, etc.)
        partitionBy: Optional column name(s) for partitioning
        
    Raises:
        ValueError: If DataFrame is empty or path is invalid
        
    Example:
        >>> df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        >>> write_parquet(df, "/path/to/output")
    """
    if df is None:
        raise ValueError("DataFrame cannot be None")
    
    row_count = df.count()
    if row_count == 0:
        logger.warning(f"Writing empty DataFrame to {path}")
    
    try:
        writer = df.write.mode(mode)
        
        if partitionBy:
            if isinstance(partitionBy, str):
                writer = writer.partitionBy(partitionBy)
            elif isinstance(partitionBy, list):
                writer = writer.partitionBy(*partitionBy)
        
        writer.parquet(path)
        logger.info(f"Written {row_count} rows to {path}")
        
    except Exception as e:
        logger.error(f"Failed to write Parquet to {path}: {e}")
        raise


def validate_output_path(output_path: str) -> None:
    """
    Validate that the output path is a non-empty string.

    Args:
        output_path: Path to validate

    Raises:
        ValueError: If output_path is None, empty, or not a string
    """
    if not output_path or not isinstance(output_path, str):
        raise ValueError("output_root must be a non-empty string")


def check_dataframe_schema(df: DataFrame, expected_columns: list) -> bool:
    """
    Validate that DataFrame contains expected columns.
    
    Args:
        df: DataFrame to check
        expected_columns: List of expected column names
        
    Returns:
        True if all expected columns exist
        
    Raises:
        ValueError: If required columns are missing
    """
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    
    missing = expected_set - actual_columns
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    return True