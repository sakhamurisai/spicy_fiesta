"""
Pytest configuration and shared fixtures for all data generator tests
Provides SparkSession, temporary output paths, and utility functions
"""

# CRITICAL: Patch typing.io BEFORE importing pyspark to avoid compatibility error with Python 3.13
import sys
import types
mock_io = types.ModuleType('io')
mock_io.BinaryIO = type('BinaryIO', (), {})
sys.modules['typing.io'] = mock_io

import pytest
import tempfile
import shutil
from pathlib import Path
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for all tests"""
    import os
    # Ensure Java is found
    os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME", "C:\\Program Files\\Java\\jdk-21")
    
    spark = SparkSession.builder \
        .appName("spicy_fiesta_tests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.sql.adaptive.enabled", "false") \
        .config("spark.default.parallelism", "1") \
        .config("spark.rdd.compress", "false") \
        .config("spark.shuffle.compress", "false") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def temp_output_dir():
    """Create temporary directory for test outputs, clean up after"""
    temp_dir = tempfile.mkdtemp(prefix="spicy_fiesta_test_")
    yield temp_dir
    # Cleanup
    if Path(temp_dir).exists():
        shutil.rmtree(temp_dir)


@pytest.fixture(scope="function")
def output_root(temp_output_dir):
    """Return output root path for generators"""
    return temp_output_dir


def assert_dataframe_columns(df, expected_columns):
    """Helper: Verify DataFrame has expected columns"""
    actual_columns = df.columns
    assert len(actual_columns) == len(expected_columns), \
        f"Column count mismatch: expected {len(expected_columns)}, got {len(actual_columns)}"
    for col in expected_columns:
        assert col in actual_columns, f"Missing column: {col}"


def assert_row_count(df, expected_count, tolerance=0):
    """Helper: Verify DataFrame row count within tolerance"""
    actual_count = df.count()
    if tolerance == 0:
        assert actual_count == expected_count, \
            f"Row count mismatch: expected {expected_count}, got {actual_count}"
    else:
        assert abs(actual_count - expected_count) <= tolerance, \
            f"Row count out of tolerance: expected {expected_count}±{tolerance}, got {actual_count}"


def assert_column_values_in_range(df, column, min_val, max_val):
    """Helper: Verify all values in column are within range"""
    result = df.filter((df[column] < min_val) | (df[column] > max_val)).count()
    assert result == 0, f"Column {column} has {result} values outside range [{min_val}, {max_val}]"


def assert_no_nulls_in_columns(df, columns):
    """Helper: Verify no NULL values in specified columns"""
    for col in columns:
        null_count = df.filter(df[col].isNull()).count()
        assert null_count == 0, f"Column {col} has {null_count} NULL values"


def assert_unique_values(df, column, expected_count):
    """Helper: Verify column has expected count of unique values"""
    unique_count = df.select(column).distinct().count()
    assert unique_count == expected_count, \
        f"Unique values mismatch in {column}: expected {expected_count}, got {unique_count}"
