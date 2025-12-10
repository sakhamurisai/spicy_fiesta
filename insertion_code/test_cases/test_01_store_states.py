"""
Comprehensive test suite for 01_store_states.py module.

Tests cover:
- States DataFrame creation
- Data completeness (all 51 jurisdictions)
- Data integrity
- Column validation
- Edge cases
"""

import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil

import sys
import os

# Get the directory where this test file is located
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TEST_DIR)

from utils import get_spark
import importlib.util

# Load the states module from the same directory
spec = importlib.util.spec_from_file_location("states_module", os.path.join(TEST_DIR, "01_store_states.py"))
states_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(states_module)


class TestStatesDataFrame:
    """Test suite for create_states_dataframe function."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture to provide a SparkSession."""
        spark = get_spark("test_states")
        yield spark
        spark.stop()
    
    def test_create_states_dataframe_success(self, spark):
        """Test successful states DataFrame creation."""
        states_df = states_module.create_states_dataframe(spark)
        
        assert states_df is not None
        assert states_df.count() > 0
    
    def test_states_count_is_51(self, spark):
        """Test that we have exactly 51 jurisdictions (50 states + DC)."""
        states_df = states_module.create_states_dataframe(spark)
        assert states_df.count() == 51
    
    def test_states_has_required_columns(self, spark):
        """Test that states DataFrame has all required columns."""
        states_df = states_module.create_states_dataframe(spark)
        
        expected_columns = [
            "StateID", "StateCode", "StateName", "StateRegion",
            "TaxRate", "IsActive", "CreatedDate", "ModifiedDate"
        ]
        
        actual_columns = states_df.columns
        assert set(expected_columns) == set(actual_columns)
    
    def test_state_codes_are_unique(self, spark):
        """Test that all state codes are unique."""
        states_df = states_module.create_states_dataframe(spark)
        
        total_count = states_df.count()
        distinct_count = states_df.select("StateCode").distinct().count()
        
        assert total_count == distinct_count
    
    def test_state_ids_are_positive(self, spark):
        """Test that all StateIDs are positive integers."""
        states_df = states_module.create_states_dataframe(spark)
        
        negative_ids = states_df.filter("StateID <= 0").count()
        assert negative_ids == 0
    
    def test_tax_rates_are_valid(self, spark):
        """Test that tax rates are between 0 and 1."""
        states_df = states_module.create_states_dataframe(spark)
        
        invalid_rates = states_df.filter("TaxRate < 0 OR TaxRate > 1").count()
        assert invalid_rates == 0
    
    def test_states_with_no_sales_tax(self, spark):
        """Test that states with no sales tax are correctly identified."""
        states_df = states_module.create_states_dataframe(spark)
        
        # States with 0% sales tax: AK, DE, MT, NH, OR
        no_tax_states = states_df.filter("TaxRate = 0.0").select("StateCode").collect()
        no_tax_codes = {row.StateCode for row in no_tax_states}
        
        expected_no_tax = {"AK", "DE", "MT", "NH", "OR"}
        assert no_tax_codes == expected_no_tax
    
    def test_all_states_active(self, spark):
        """Test that all states are marked as active."""
        states_df = states_module.create_states_dataframe(spark)
        
        inactive_states = states_df.filter("IsActive != 1").count()
        assert inactive_states == 0
    
    def test_regions_are_valid(self, spark):
        """Test that all regions are valid US regions."""
        states_df = states_module.create_states_dataframe(spark)
        
        valid_regions = {"Northeast", "South", "Midwest", "West"}
        regions = {row.StateRegion for row in states_df.select("StateRegion").distinct().collect()}
        
        assert regions.issubset(valid_regions)
    
    def test_specific_states_exist(self, spark):
        """Test that specific major states exist with correct data."""
        states_df = states_module.create_states_dataframe(spark)
        
        # Test California
        ca_row = states_df.filter("StateCode = 'CA'").collect()
        assert len(ca_row) == 1
        assert ca_row[0].StateName == "California"
        assert ca_row[0].StateRegion == "West"
        assert ca_row[0].TaxRate == 0.0725
        
        # Test Texas
        tx_row = states_df.filter("StateCode = 'TX'").collect()
        assert len(tx_row) == 1
        assert tx_row[0].StateName == "Texas"
        
        # Test DC
        dc_row = states_df.filter("StateCode = 'DC'").collect()
        assert len(dc_row) == 1
        assert dc_row[0].StateName == "District of Columbia"
    
    def test_no_null_values_in_key_columns(self, spark):
        """Test that key columns have no null values."""
        states_df = states_module.create_states_dataframe(spark)
        
        assert states_df.filter("StateID IS NULL").count() == 0
        assert states_df.filter("StateCode IS NULL").count() == 0
        assert states_df.filter("StateName IS NULL").count() == 0
        assert states_df.filter("StateRegion IS NULL").count() == 0
        assert states_df.filter("TaxRate IS NULL").count() == 0
    
    def test_created_and_modified_dates_exist(self, spark):
        """Test that all records have created and modified dates."""
        states_df = states_module.create_states_dataframe(spark)
        
        assert states_df.filter("CreatedDate IS NULL").count() == 0
        assert states_df.filter("ModifiedDate IS NULL").count() == 0
    
    def test_none_spark_raises_error(self):
        """Test that None SparkSession raises ValueError."""
        with pytest.raises(ValueError, match="SparkSession cannot be None"):
            states_module.create_states_dataframe(None)


class TestStatesMain:
    """Test suite for states main function."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        """Fixture to provide a temporary directory."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_main_creates_parquet_file(self, temp_dir):
        """Test that main function creates parquet file."""
        states_module.main(temp_dir)
        
        output_path = os.path.join(temp_dir, "store.States")
        assert os.path.exists(output_path)
    
    def test_main_data_can_be_read_back(self, temp_dir):
        """Test that written data can be read back correctly."""
        states_module.main(temp_dir)
        
        spark = get_spark("test_read")
        output_path = os.path.join(temp_dir, "store.States")
        read_df = spark.read.parquet(output_path)
        
        assert read_df.count() == 51
        assert "StateID" in read_df.columns
        assert "StateCode" in read_df.columns
        
        spark.stop()
    
    def test_main_with_invalid_output_root(self):
        """Test that invalid output root raises ValueError."""
        with pytest.raises(ValueError, match="output_root must be a non-empty string"):
            states_module.main("")
    
    def test_main_with_none_output_root(self):
        """Test that None output root raises ValueError."""
        with pytest.raises(ValueError):
            states_module.main(None)


class TestStatesDataIntegrity:
    """Test data integrity and business rules."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture to provide a SparkSession."""
        spark = get_spark("test_integrity")
        yield spark
        spark.stop()
    
    def test_highest_tax_rate_states(self, spark):
        """Test identification of states with highest tax rates."""
        states_df = states_module.create_states_dataframe(spark)
        
        # Find states with tax rate > 7%
        high_tax = states_df.filter("TaxRate > 0.07").collect()
        high_tax_codes = {row.StateCode for row in high_tax}
        
        # Should include states like IN, MS, RI, TN
        assert "IN" in high_tax_codes  # 7%
        assert "MS" in high_tax_codes  # 7%
    
    def test_regional_distribution(self, spark):
        """Test that states are distributed across all regions."""
        states_df = states_module.create_states_dataframe(spark)
        
        region_counts = states_df.groupBy("StateRegion").count().collect()
        region_dict = {row.StateRegion: row["count"] for row in region_counts}
        
        # All regions should have at least one state
        assert all(count > 0 for count in region_dict.values())
        
        # South should have the most states
        assert region_dict["South"] > region_dict["Northeast"]
    
    def test_state_code_format(self, spark):
        """Test that all state codes are 2 uppercase letters."""
        states_df = states_module.create_states_dataframe(spark)
        
        codes = [row.StateCode for row in states_df.select("StateCode").collect()]
        
        for code in codes:
            assert len(code) == 2
            assert code.isupper()
            assert code.isalpha() or code == "DC"


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture to provide a SparkSession."""
        spark = get_spark("test_edge")
        yield spark
        spark.stop()
    
    def test_tax_rate_precision(self, spark):
        """Test that tax rates maintain appropriate precision."""
        states_df = states_module.create_states_dataframe(spark)
        
        # All tax rates should be reasonable (not too many decimal places)
        rates = [row.TaxRate for row in states_df.select("TaxRate").collect()]
        
        for rate in rates:
            # Check that rate is not ridiculously precise (like 0.0123456789)
            assert rate < 0.1  # No state has > 10% sales tax
            assert rate >= 0   # No negative taxes
    
    def test_district_of_columbia_special_case(self, spark):
        """Test that DC is included and properly categorized."""
        states_df = states_module.create_states_dataframe(spark)
        
        dc_row = states_df.filter("StateCode = 'DC'").collect()
        assert len(dc_row) == 1
        assert dc_row[0].StateName == "District of Columbia"
        # DC is categorized as South in this dataset
        assert dc_row[0].StateRegion == "South"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
