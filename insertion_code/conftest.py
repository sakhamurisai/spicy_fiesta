"""
Pytest configuration and shared fixtures.
"""

import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil
import sys

# Add corrected_code to path
sys.path.insert(0, '/home/claude/corrected_code')


@pytest.fixture(scope="session")
def spark_session():
    """Provide a SparkSession for the entire test session."""
    spark = (
        SparkSession.builder
        .appName("pytest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def temp_output_dir():
    """Provide a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)
