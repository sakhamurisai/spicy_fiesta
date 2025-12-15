"""
Utility functions for data insertion pipeline - Databricks Compatible.

This module provides common functionality for Spark operations
optimized for Databricks environment with shared Spark sessions.
"""
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_spark(app_name: str, master: str = None) -> SparkSession:
    """
    Get the shared Spark session in Databricks.
    
    IMPORTANT: In Databricks, this ALWAYS returns the shared SparkSession.
    Do NOT create new sessions - use the existing one.
    
    Args:
        app_name: Name for logging purposes (not used for session creation)
        master: Ignored in Databricks (uses shared cluster)
        
    Returns:
        Shared SparkSession instance from Databricks
        
    Example:
        >>> spark = get_spark("my_app")
        >>> df = spark.range(10)
    """
    try:
        # In Databricks, ALWAYS use getOrCreate() which returns the shared session
        # Do NOT use builder.master() or other configs - they're ignored
        spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
        
        logger.info(f"Using shared Databricks Spark session for: {app_name}")
        return spark
        
    except Exception as e:
        logger.error(f"Failed to get Spark session: {e}")
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
        path: Output path for Parquet files (Azure blob path or DBFS)
        mode: Write mode (overwrite, append, etc.)
        partitionBy: Optional column name(s) for partitioning
        
    Raises:
        ValueError: If DataFrame is empty or path is invalid
        
    Example:
        >>> df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        >>> write_parquet(df, "/mnt/data/output")
    """
    if df is None:
        raise ValueError("DataFrame cannot be None")
    
    try:
        row_count = df.count()
        if row_count == 0:
            logger.warning(f"Writing empty DataFrame to {path}")
        
        writer = df.write.mode(mode)
        
        if partitionBy:
            if isinstance(partitionBy, str):
                writer = writer.partitionBy(partitionBy)
            elif isinstance(partitionBy, list):
                writer = writer.partitionBy(*partitionBy)
        
        writer.parquet(path)
        logger.info(f"Successfully written {row_count} rows to {path}")
        
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
    
    logger.info(f"Schema validation passed: all {len(expected_columns)} columns present")
    return True
