"""Comprehensive test suite for 02_store_locations.py module."""

import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil
import os
import sys

sys.path.insert(0, '/home/claude/corrected_code')
from utils import get_spark
import importlib.util

# Load modules
spec_states = importlib.util.spec_from_file_location("states_module", "/home/claude/corrected_code/01_store_states.py")
states_module = importlib.util.module_from_spec(spec_states)
spec_states.loader.exec_module(states_module)

spec_locations = importlib.util.spec_from_file_location("locations_module", "/home/claude/corrected_code/02_store_locations.py")
locations_module = importlib.util.module_from_spec(spec_locations)
spec_locations.loader.exec_module(locations_module)


class TestLocationsDataFrame:
    """Test suite for create_locations_dataframe function."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = get_spark("test_locations")
        yield spark
        spark.stop()
    
    @pytest.fixture(scope="class")
    def states_df(self, spark):
        return states_module.create_states_dataframe(spark)
    
    def test_create_locations_success(self, spark, states_df):
        """Test successful locations DataFrame creation."""
        locations_df = locations_module.create_locations_dataframe(spark, 10, states_df)
        assert locations_df is not None
        assert locations_df.count() == 10
    
    def test_locations_has_required_columns(self, spark, states_df):
        """Test that locations DataFrame has all required columns."""
        locations_df = locations_module.create_locations_dataframe(spark, 5, states_df)
        
        expected_columns = [
            "LocationID", "LocationGUID", "StoreNumber", "StoreName", "AddressLine1",
            "City", "StateID", "ZipCode", "PhoneNumber", "StoreType", "HasDriveThru",
            "HasDineIn", "OpeningDateID", "ClosingDateID", "IsActive", "CreatedDate",
            "ModifiedDate", "CreatedBy", "ModifiedBy"
        ]
        
        assert set(expected_columns) == set(locations_df.columns)
    
    def test_location_ids_are_unique(self, spark, states_df):
        """Test that all LocationIDs are unique."""
        locations_df = locations_module.create_locations_dataframe(spark, 50, states_df)
        
        total = locations_df.count()
        distinct = locations_df.select("LocationID").distinct().count()
        assert total == distinct
    
    def test_store_numbers_are_unique(self, spark, states_df):
        """Test that all StoreNumbers are unique."""
        locations_df = locations_module.create_locations_dataframe(spark, 50, states_df)
        
        total = locations_df.count()
        distinct = locations_df.select("StoreNumber").distinct().count()
        assert total == distinct
    
    def test_location_guids_are_unique(self, spark, states_df):
        """Test that all LocationGUIDs are unique."""
        locations_df = locations_module.create_locations_dataframe(spark, 20, states_df)
        
        total = locations_df.count()
        distinct = locations_df.select("LocationGUID").distinct().count()
        assert total == distinct
    
    def test_all_state_ids_valid(self, spark, states_df):
        """Test that all StateIDs reference valid states."""
        locations_df = locations_module.create_locations_dataframe(spark, 100, states_df)
        
        state_ids = {row.StateID for row in states_df.select("StateID").collect()}
        location_state_ids = {row.StateID for row in locations_df.select("StateID").collect()}
        
        assert location_state_ids.issubset(state_ids)
    
    def test_all_locations_active(self, spark, states_df):
        """Test that all locations are marked as active."""
        locations_df = locations_module.create_locations_dataframe(spark, 30, states_df)
        
        inactive = locations_df.filter("IsActive != 1").count()
        assert inactive == 0
    
    def test_store_type_is_standalone(self, spark, states_df):
        """Test that all stores are marked as Standalone."""
        locations_df = locations_module.create_locations_dataframe(spark, 20, states_df)
        
        non_standalone = locations_df.filter("StoreType != 'Standalone'").count()
        assert non_standalone == 0
    
    def test_all_have_drive_thru(self, spark, states_df):
        """Test that all stores have drive thru."""
        locations_df = locations_module.create_locations_dataframe(spark, 20, states_df)
        
        without_drive_thru = locations_df.filter("HasDriveThru != 1").count()
        assert without_drive_thru == 0
    
    def test_all_have_dine_in(self, spark, states_df):
        """Test that all stores have dine in."""
        locations_df = locations_module.create_locations_dataframe(spark, 20, states_df)
        
        without_dine_in = locations_df.filter("HasDineIn != 1").count()
        assert without_dine_in == 0
    
    def test_store_number_format(self, spark, states_df):
        """Test that store numbers follow SF-XXXXX format."""
        locations_df = locations_module.create_locations_dataframe(spark, 5, states_df)
        
        store_numbers = [row.StoreNumber for row in locations_df.select("StoreNumber").collect()]
        
        for sn in store_numbers:
            assert sn.startswith("SF-")
            assert len(sn) == 8  # SF-XXXXX
            assert sn[3:].isdigit()
    
    def test_zip_code_format(self, spark, states_df):
        """Test that zip codes are 5 digits."""
        locations_df = locations_module.create_locations_dataframe(spark, 10, states_df)
        
        zip_codes = [row.ZipCode for row in locations_df.select("ZipCode").collect()]
        
        for zc in zip_codes:
            assert len(zc) == 5
            assert zc.isdigit()
    
    def test_no_null_required_fields(self, spark, states_df):
        """Test that required fields have no nulls."""
        locations_df = locations_module.create_locations_dataframe(spark, 20, states_df)
        
        assert locations_df.filter("LocationID IS NULL").count() == 0
        assert locations_df.filter("StoreNumber IS NULL").count() == 0
        assert locations_df.filter("StoreName IS NULL").count() == 0
        assert locations_df.filter("StateID IS NULL").count() == 0
    
    def test_created_by_is_system(self, spark, states_df):
        """Test that CreatedBy is 'system'."""
        locations_df = locations_module.create_locations_dataframe(spark, 10, states_df)
        
        non_system = locations_df.filter("CreatedBy != 'system'").count()
        assert non_system == 0
    
    def test_zero_locations_raises_error(self, spark, states_df):
        """Test that zero locations raises ValueError."""
        with pytest.raises(ValueError):
            locations_module.create_locations_dataframe(spark, 0, states_df)
    
    def test_negative_locations_raises_error(self, spark, states_df):
        """Test that negative locations raises ValueError."""
        with pytest.raises(ValueError):
            locations_module.create_locations_dataframe(spark, -5, states_df)
    
    def test_none_spark_raises_error(self, states_df):
        """Test that None SparkSession raises ValueError."""
        with pytest.raises(ValueError):
            locations_module.create_locations_dataframe(None, 10, states_df)
    
    def test_none_states_df_raises_error(self, spark):
        """Test that None states DataFrame raises ValueError."""
        with pytest.raises(ValueError):
            locations_module.create_locations_dataframe(spark, 10, None)


class TestLocationsMain:
    """Test suite for locations main function."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        # Create prerequisite states table
        spark = get_spark("setup")
        states_df = states_module.create_states_dataframe(spark)
        states_module.write_parquet(states_df, f"{temp_path}/store.States")
        spark.stop()
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_main_creates_parquet_file(self, temp_dir):
        """Test that main function creates parquet file."""
        locations_module.main(temp_dir, 10)
        
        output_path = os.path.join(temp_dir, "store.Locations")
        assert os.path.exists(output_path)
    
    def test_main_correct_record_count(self, temp_dir):
        """Test that correct number of records are created."""
        num_locations = 25
        locations_module.main(temp_dir, num_locations)
        
        spark = get_spark("test_read")
        output_path = os.path.join(temp_dir, "store.Locations")
        read_df = spark.read.parquet(output_path)
        
        assert read_df.count() == num_locations
        spark.stop()
    
    def test_main_with_invalid_output_root(self):
        """Test that invalid output root raises ValueError."""
        with pytest.raises(ValueError):
            locations_module.main("", 10)


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = get_spark("test_edge")
        yield spark
        spark.stop()
    
    @pytest.fixture(scope="class")
    def states_df(self, spark):
        return states_module.create_states_dataframe(spark)
    
    def test_single_location(self, spark, states_df):
        """Test creating a single location."""
        locations_df = locations_module.create_locations_dataframe(spark, 1, states_df)
        assert locations_df.count() == 1
    
    def test_large_number_of_locations(self, spark, states_df):
        """Test creating many locations."""
        locations_df = locations_module.create_locations_dataframe(spark, 1000, states_df)
        assert locations_df.count() == 1000
    
    def test_state_distribution(self, spark, states_df):
        """Test that locations are distributed across states."""
        locations_df = locations_module.create_locations_dataframe(spark, 500, states_df)
        
        distinct_states = locations_df.select("StateID").distinct().count()
        # Should have multiple states represented
        assert distinct_states > 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
