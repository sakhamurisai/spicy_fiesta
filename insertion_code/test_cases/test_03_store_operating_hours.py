"""
Comprehensive test suite for 03_store_operating_hours.py module.

Tests cover:
- Operating hours DataFrame creation
- All days of week coverage
- Data integrity validation
- Edge cases and error conditions
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

# Load modules
import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


states_mod = load_module("states", os.path.join(TEST_DIR, "01_store_states.py"))
locations_mod = load_module("locations", os.path.join(TEST_DIR, "02_store_locations.py"))
hours_mod = load_module("hours", os.path.join(TEST_DIR, "03_store_operating_hours.py"))


class TestOperatingHoursDataFrame:
    """Test suite for create_operating_hours_dataframe function."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = get_spark("test_hours")
        yield spark
        spark.stop()
    
    @pytest.fixture(scope="class")
    def locations_df(self, spark):
        temp_dir = tempfile.mkdtemp()
        states_mod.main(temp_dir)
        locations_mod.main(temp_dir, 5)
        df = spark.read.parquet(f"{temp_dir}/store.Locations").select("LocationID")
        shutil.rmtree(temp_dir, ignore_errors=True)
        return df
    
    def test_create_hours_success(self, spark, locations_df):
        """Test successful operating hours creation."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        assert hours_df is not None
        assert hours_df.count() > 0
    
    def test_hours_has_required_columns(self, spark, locations_df):
        """Test that all required columns exist."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        expected_columns = [
            "OperatingHoursID", "LocationID", "DayOfWeek", "OpenTime", 
            "CloseTime", "IsHoliday", "HolidayName", "EffectiveDateID", 
            "ExpiryDateID", "CreatedDate"
        ]
        
        assert set(expected_columns) == set(hours_df.columns)
    
    def test_seven_days_per_location(self, spark, locations_df):
        """Test that each location has 7 days of operating hours."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        num_locations = locations_df.count()
        total_hours = hours_df.count()
        
        assert total_hours == num_locations * 7
    
    def test_day_of_week_range(self, spark, locations_df):
        """Test that DayOfWeek is between 1 and 7."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        invalid_days = hours_df.filter("DayOfWeek < 1 OR DayOfWeek > 7").count()
        assert invalid_days == 0
    
    def test_operating_hours_id_unique(self, spark, locations_df):
        """Test that OperatingHoursID is unique."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        total = hours_df.count()
        distinct = hours_df.select("OperatingHoursID").distinct().count()
        
        assert total == distinct
    
    def test_default_open_time(self, spark, locations_df):
        """Test that default open time is 08:00:00."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        non_default = hours_df.filter("OpenTime != '08:00:00'").count()
        assert non_default == 0
    
    def test_default_close_time(self, spark, locations_df):
        """Test that default close time is 22:00:00."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        non_default = hours_df.filter("CloseTime != '22:00:00'").count()
        assert non_default == 0
    
    def test_no_holidays_by_default(self, spark, locations_df):
        """Test that IsHoliday is 0 for all records."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        holidays = hours_df.filter("IsHoliday != 0").count()
        assert holidays == 0
    
    def test_effective_date_id_is_one(self, spark, locations_df):
        """Test that EffectiveDateID is 1."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        non_one = hours_df.filter("EffectiveDateID != 1").count()
        assert non_one == 0
    
    def test_no_nulls_in_required_fields(self, spark, locations_df):
        """Test no nulls in required fields."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        assert hours_df.filter("OperatingHoursID IS NULL").count() == 0
        assert hours_df.filter("LocationID IS NULL").count() == 0
        assert hours_df.filter("DayOfWeek IS NULL").count() == 0
        assert hours_df.filter("OpenTime IS NULL").count() == 0
        assert hours_df.filter("CloseTime IS NULL").count() == 0
    
    def test_none_spark_raises_error(self, locations_df):
        """Test that None spark raises ValueError."""
        with pytest.raises(ValueError, match="SparkSession cannot be None"):
            hours_mod.create_operating_hours_dataframe(None, locations_df)
    
    def test_none_locations_raises_error(self, spark):
        """Test that None locations raises ValueError."""
        with pytest.raises(ValueError, match="cannot be None or empty"):
            hours_mod.create_operating_hours_dataframe(spark, None)
    
    def test_each_location_has_all_days(self, spark, locations_df):
        """Test that each location has records for all 7 days."""
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        location_ids = [row.LocationID for row in locations_df.collect()]
        
        for loc_id in location_ids:
            days_for_location = hours_df.filter(f"LocationID = {loc_id}").count()
            assert days_for_location == 7


class TestOperatingHoursMain:
    """Test suite for main function."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        
        # Create prerequisites
        states_mod.main(temp_path)
        locations_mod.main(temp_path, 3)
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_main_creates_parquet(self, temp_dir):
        """Test that main creates parquet file."""
        hours_mod.main(temp_dir)
        
        output_path = os.path.join(temp_dir, "store.OperatingHours")
        assert os.path.exists(output_path)
    
    def test_main_correct_record_count(self, temp_dir):
        """Test that correct number of records are created."""
        hours_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        hours_df = spark.read.parquet(f"{temp_dir}/store.OperatingHours")
        
        # 3 locations * 7 days = 21 records
        assert hours_df.count() == 21
        spark.stop()
    
    def test_main_with_invalid_output_root(self):
        """Test that invalid output root raises ValueError."""
        with pytest.raises(ValueError):
            hours_mod.main("")


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = get_spark("test_edge")
        yield spark
        spark.stop()
    
    def test_single_location(self, spark):
        """Test with single location."""
        temp_dir = tempfile.mkdtemp()
        states_mod.main(temp_dir)
        locations_mod.main(temp_dir, 1)
        
        locations_df = spark.read.parquet(f"{temp_dir}/store.Locations").select("LocationID")
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        assert hours_df.count() == 7
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_many_locations(self, spark):
        """Test with many locations."""
        temp_dir = tempfile.mkdtemp()
        states_mod.main(temp_dir)
        locations_mod.main(temp_dir, 50)
        
        locations_df = spark.read.parquet(f"{temp_dir}/store.Locations").select("LocationID")
        hours_df = hours_mod.create_operating_hours_dataframe(spark, locations_df)
        
        assert hours_df.count() == 350  # 50 * 7
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
