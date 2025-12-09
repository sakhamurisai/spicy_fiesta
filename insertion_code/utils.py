"""
Utility functions for Spark DataFrame operations and Parquet file handling.

This module provides reusable functions for creating Spark sessions and
writing DataFrames to Parquet format with consistent configuration.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame


def get_spark(app_name: str = "spicy-fiesta") -> SparkSession:
    """
    Create and configure a SparkSession with optimized settings.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        Configured SparkSession instance
        
    Raises:
        ValueError: If app_name is empty or invalid
    """
    if not app_name or not isinstance(app_name, str):
        raise ValueError("app_name must be a non-empty string")
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()
    )
    return spark


def write_parquet(
    df: DataFrame,
    path: str,
    partitionBy: Optional[str] = None,
    mode: str = "overwrite"
) -> None:
    """
    Write DataFrame to Parquet format with optional partitioning.
    
    Args:
        df: DataFrame to write
        path: Output path for Parquet files
        partitionBy: Optional column name for partitioning
        mode: Write mode (default: "overwrite")
        
    Raises:
        ValueError: If DataFrame is None, empty, or path is invalid
        TypeError: If df is not a DataFrame
    """
    if df is None:
        raise ValueError("DataFrame cannot be None")
    
    if not isinstance(df, DataFrame):
        raise TypeError("df must be a PySpark DataFrame")
    
    if not path or not isinstance(path, str):
        raise ValueError("Path must be a non-empty string")
    
    if df.rdd.isEmpty():
        raise ValueError("DataFrame is empty, cannot write to Parquet")
    
    writer = df.write.mode(mode)
    
    if partitionBy:
        if not isinstance(partitionBy, str):
            raise ValueError("partitionBy must be a string")
        writer = writer.partitionBy(partitionBy)
    
    writer.parquet(path)
