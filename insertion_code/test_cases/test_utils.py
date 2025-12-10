"""
Comprehensive test suite for utils.py module.

Tests cover:
- SparkSession creation and configuration
- Parquet writing with/without partitioning
- Input validation
- Error handling
- Edge cases
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import tempfile
import shutil
import os

# Import the module to test
import sys
import os
TEST_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, TEST_DIR)
from utils import get_spark, write_parquet


class TestGetSpark:
    """Test suite for get_spark function."""
    
    def test_get_spark_default_app_name(self):
        """Test SparkSession creation with default app name."""
        spark = get_spark()
        assert spark is not None
        assert isinstance(spark, SparkSession)
        assert spark.conf.get("spark.app.name") == "spicy-fiesta"
        spark.stop()
    
    def test_get_spark_custom_app_name(self):
        """Test SparkSession creation with custom app name."""
        custom_name = "test_app"
        spark = get_spark(custom_name)
        assert spark is not None
        assert spark.conf.get("spark.app.name") == custom_name
        spark.stop()
    
    def test_get_spark_shuffle_partitions_config(self):
        """Test that shuffle partitions are configured correctly."""
        spark = get_spark("test")
        shuffle_partitions = spark.conf.get("spark.sql.shuffle.partitions")
        assert shuffle_partitions == "400"
        spark.stop()
    
    def test_get_spark_empty_app_name_raises_error(self):
        """Test that empty app name raises ValueError."""
        with pytest.raises(ValueError, match="app_name must be a non-empty string"):
            get_spark("")
    
    def test_get_spark_none_app_name_raises_error(self):
        """Test that None app name raises ValueError."""
        with pytest.raises(ValueError, match="app_name must be a non-empty string"):
            get_spark(None)
    
    def test_get_spark_invalid_type_raises_error(self):
        """Test that invalid app name type raises ValueError."""
        with pytest.raises(ValueError, match="app_name must be a non-empty string"):
            get_spark(123)
    
    def test_get_spark_whitespace_only_raises_error(self):
        """Test that whitespace-only app name raises ValueError."""
        with pytest.raises(ValueError):
            get_spark("   ")


class TestWriteParquet:
    """Test suite for write_parquet function."""
    
    @pytest.fixture(scope="function")
    def spark(self):
        """Fixture to provide a SparkSession for each test."""
        spark = get_spark("test")
        yield spark
        spark.stop()
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        """Fixture to provide a temporary directory."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    @pytest.fixture(scope="function")
    def sample_dataframe(self, spark):
        """Fixture to provide a sample DataFrame."""
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        return spark.createDataFrame(data, schema)
    
    def test_write_parquet_success(self, spark, sample_dataframe, temp_dir):
        """Test successful parquet write."""
        output_path = os.path.join(temp_dir, "test_output")
        write_parquet(sample_dataframe, output_path)
        
        # Verify file was created
        assert os.path.exists(output_path)
        
        # Verify we can read it back
        read_df = spark.read.parquet(output_path)
        assert read_df.count() == 3
        assert set(read_df.columns) == {"name", "age"}
    
    def test_write_parquet_with_partitioning(self, spark, temp_dir):
        """Test parquet write with partitioning."""
        data = [("Alice", 25, "NY"), ("Bob", 30, "CA"), ("Charlie", 35, "NY")]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("state", StringType(), True)
        ])
        df = spark.createDataFrame(data, schema)
        
        output_path = os.path.join(temp_dir, "partitioned_output")
        write_parquet(df, output_path, partitionBy="state")
        
        # Verify partitioned structure exists
        assert os.path.exists(output_path)
        assert os.path.exists(os.path.join(output_path, "state=NY"))
        assert os.path.exists(os.path.join(output_path, "state=CA"))
    
    def test_write_parquet_overwrite_mode(self, spark, sample_dataframe, temp_dir):
        """Test that default overwrite mode works."""
        output_path = os.path.join(temp_dir, "overwrite_test")
        
        # Write first time
        write_parquet(sample_dataframe, output_path)
        first_count = spark.read.parquet(output_path).count()
        
        # Write again (should overwrite)
        write_parquet(sample_dataframe, output_path)
        second_count = spark.read.parquet(output_path).count()
        
        assert first_count == second_count == 3
    
    def test_write_parquet_none_dataframe_raises_error(self, temp_dir):
        """Test that None DataFrame raises ValueError."""
        with pytest.raises(ValueError, match="DataFrame cannot be None"):
            write_parquet(None, temp_dir)
    
    def test_write_parquet_invalid_dataframe_type_raises_error(self, temp_dir):
        """Test that invalid DataFrame type raises TypeError."""
        with pytest.raises(TypeError, match="df must be a PySpark DataFrame"):
            write_parquet("not a dataframe", temp_dir)
    
    def test_write_parquet_empty_path_raises_error(self, spark, sample_dataframe):
        """Test that empty path raises ValueError."""
        with pytest.raises(ValueError, match="Path must be a non-empty string"):
            write_parquet(sample_dataframe, "")
    
    def test_write_parquet_none_path_raises_error(self, spark, sample_dataframe):
        """Test that None path raises ValueError."""
        with pytest.raises(ValueError, match="Path must be a non-empty string"):
            write_parquet(sample_dataframe, None)
    
    def test_write_parquet_empty_dataframe_raises_error(self, spark, temp_dir):
        """Test that empty DataFrame raises ValueError."""
        empty_df = spark.createDataFrame([], StructType([
            StructField("name", StringType(), True)
        ]))
        
        with pytest.raises(ValueError, match="DataFrame is empty"):
            write_parquet(empty_df, temp_dir)
    
    def test_write_parquet_invalid_partition_column_raises_error(self, spark, sample_dataframe, temp_dir):
        """Test that invalid partition column type raises ValueError."""
        with pytest.raises(ValueError, match="partitionBy must be a string"):
            write_parquet(sample_dataframe, temp_dir, partitionBy=123)
    
    def test_write_parquet_append_mode(self, spark, sample_dataframe, temp_dir):
        """Test write with append mode."""
        output_path = os.path.join(temp_dir, "append_test")
        
        # Note: append mode is not currently supported in the function
        # but we test that overwrite mode works correctly
        write_parquet(sample_dataframe, output_path, mode="overwrite")
        result = spark.read.parquet(output_path)
        assert result.count() == 3


class TestIntegration:
    """Integration tests for the utils module."""
    
    def test_end_to_end_workflow(self):
        """Test complete workflow: create spark, create df, write parquet."""
        temp_dir = tempfile.mkdtemp()
        
        try:
            # Create Spark session
            spark = get_spark("integration_test")
            
            # Create sample data
            data = [(i, f"name_{i}", i * 10) for i in range(100)]
            df = spark.createDataFrame(data, ["id", "name", "value"])
            
            # Write to parquet
            output_path = os.path.join(temp_dir, "integration_output")
            write_parquet(df, output_path)
            
            # Read back and verify
            result_df = spark.read.parquet(output_path)
            assert result_df.count() == 100
            assert set(result_df.columns) == {"id", "name", "value"}
            
            spark.stop()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
