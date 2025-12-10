"""
Comprehensive test suite for 04_emp_positions.py module.
"""

import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil
import os
import sys

import os
TEST_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, TEST_DIR)
from utils import get_spark
import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

positions_mod = load_module("positions", os.path.join(TEST_DIR, "04_emp_positions.py"))


class TestPositionsDataFrame:
    """Test suite for create_positions_dataframe function."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = get_spark("test_positions")
        yield spark
        spark.stop()
    
    def test_create_positions_success(self, spark):
        """Test successful positions creation."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        assert positions_df is not None
        assert positions_df.count() == 4
    
    def test_positions_has_required_columns(self, spark):
        """Test all required columns exist."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        expected_columns = [
            "PositionID", "PositionCode", "PositionName", "PositionLevel",
            "Department", "MinHourlyRate", "MaxHourlyRate", "RequiresCertification",
            "IsActive", "CreatedDate"
        ]
        
        assert set(expected_columns) == set(positions_df.columns)
    
    def test_position_ids_unique(self, spark):
        """Test that PositionIDs are unique."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        total = positions_df.count()
        distinct = positions_df.select("PositionID").distinct().count()
        assert total == distinct
    
    def test_position_codes_unique(self, spark):
        """Test that PositionCodes are unique."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        total = positions_df.count()
        distinct = positions_df.select("PositionCode").distinct().count()
        assert total == distinct
    
    def test_min_rate_less_than_max_rate(self, spark):
        """Test that MinHourlyRate < MaxHourlyRate."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        invalid = positions_df.filter("MinHourlyRate >= MaxHourlyRate").count()
        assert invalid == 0
    
    def test_hourly_rates_positive(self, spark):
        """Test that hourly rates are positive."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        assert positions_df.filter("MinHourlyRate <= 0").count() == 0
        assert positions_df.filter("MaxHourlyRate <= 0").count() == 0
    
    def test_position_levels_sequential(self, spark):
        """Test that position levels are sequential."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        levels = sorted([row.PositionLevel for row in positions_df.select("PositionLevel").collect()])
        assert levels == [1, 2, 3, 4]
    
    def test_all_positions_active(self, spark):
        """Test that all positions are active."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        inactive = positions_df.filter("IsActive != 1").count()
        assert inactive == 0
    
    def test_specific_positions_exist(self, spark):
        """Test that specific positions exist with correct data."""
        positions_df = positions_mod.create_positions_dataframe(spark)
        
        crew = positions_df.filter("PositionCode = 'POS-01'").collect()
        assert len(crew) == 1
        assert crew[0].PositionName == "Crew Member"
        assert crew[0].Department == "Operations"
    
    def test_none_spark_raises_error(self):
        """Test that None spark raises ValueError."""
        with pytest.raises(ValueError, match="SparkSession cannot be None"):
            positions_mod.create_positions_dataframe(None)


class TestPositionsMain:
    """Test suite for main function."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_main_creates_parquet(self, temp_dir):
        """Test that main creates parquet file."""
        positions_mod.main(temp_dir)
        
        output_path = os.path.join(temp_dir, "emp.Positions")
        assert os.path.exists(output_path)
    
    def test_main_correct_count(self, temp_dir):
        """Test correct number of positions created."""
        positions_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        positions_df = spark.read.parquet(f"{temp_dir}/emp.Positions")
        assert positions_df.count() == 4
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
