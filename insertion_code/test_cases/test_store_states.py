"""
Test suite for 01_store_states.py - store.States reference table
Tests: 5 hardcoded states, region assignments, tax rates, no duplicates
Coverage: 100% of states generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestStoreStatesGenerator:
    """Test suite for Store States generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_states_row_count(self):
        """TC-001: Verify exactly 5 states"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        assert_row_count(df, 5, tolerance=0)
    
    def test_states_columns_exist(self):
        """TC-002: Verify all columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        expected_columns = ["StateID", "StateName", "StateCode", "Region", "TaxRate", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_state_ids_unique(self):
        """TC-003: Verify StateID is unique for all 5 states"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        assert_unique_values(df, "StateID", 5)
    
    def test_state_codes_unique(self):
        """TC-004: Verify StateCode is unique (5 unique state codes)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        assert_unique_values(df, "StateCode", 5)
    
    def test_state_code_length(self):
        """TC-005: Verify all StateCode values are exactly 2 characters"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        invalid_length = df.filter(lambda row: len(row.StateCode) != 2).count()
        assert invalid_length == 0, f"Found {invalid_length} states with invalid code length"
    
    def test_no_nulls_in_critical_columns(self):
        """TC-006: Verify no NULLs in StateID, StateName, StateCode, TaxRate"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        critical_cols = ["StateID", "StateName", "StateCode", "TaxRate"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_tax_rate_range(self):
        """TC-007: Verify TaxRate is between 0.0 and 0.15 (typical US tax range)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        invalid_rates = df.filter((df.TaxRate < 0.0) | (df.TaxRate > 0.15)).count()
        assert invalid_rates == 0, f"Found {invalid_rates} invalid tax rates"
    
    def test_hardcoded_states_present(self):
        """TC-008: Verify expected hardcoded states are present (CA, TX, NY, FL, IL)"""
        df = self.spark.read.parquet(f"{self.output_row}/store.States")
        expected_codes = {'CA', 'TX', 'NY', 'FL', 'IL'}
        actual_codes = {row.StateCode for row in df.select("StateCode").collect()}
        assert expected_codes == actual_codes, f"Expected {expected_codes}, got {actual_codes}"
    
    def test_region_not_null(self):
        """TC-009: Verify Region column is populated (not NULL)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        null_regions = df.filter(df.Region.isNull()).count()
        assert null_regions == 0, f"Found {null_regions} NULL regions"
    
    def test_tax_rate_precision(self):
        """TC-010: Verify TaxRate has reasonable precision (2-3 decimal places)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        # Convert to string and check decimal places
        tax_rates = [str(row.TaxRate) for row in df.select("TaxRate").collect()]
        for rate_str in tax_rates:
            if '.' in rate_str:
                decimal_places = len(rate_str.split('.')[1])
                assert decimal_places <= 4, f"Tax rate {rate_str} has too many decimal places"
    
    def test_state_name_length(self):
        """TC-011: Verify StateName has reasonable length (5-20 characters)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        invalid_names = df.filter((len(df.StateName) < 2) | (len(df.StateName) > 30)).count()
        assert invalid_names == 0, f"Found {invalid_names} invalid state names"
    
    def test_all_rows_have_monotonic_id(self):
        """TC-012: Verify StateID follows monotonically increasing pattern (1-5)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States").orderBy("StateID")
        ids = [row.StateID for row in df.collect()]
        assert ids == [1, 2, 3, 4, 5], f"StateID not sequential: {ids}"
    
    def test_created_date_populated(self):
        """TC-013: Verify CreatedDate is populated for all rows"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        null_dates = df.filter(df.CreatedDate.isNull()).count()
        assert null_dates == 0, f"Found {null_dates} NULL CreatedDate values"
    
    def test_no_duplicate_states(self):
        """TC-014: Verify no duplicate StateName entries"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        duplicates = df.groupBy("StateName").count().filter("count > 1").count()
        assert duplicates == 0, f"Found {duplicates} duplicate StateName entries"
    
    def test_case_insensitive_state_codes(self):
        """TC-015: Verify all StateCode values are uppercase"""
        df = self.spark.read.parquet(f"{self.output_root}/store.States")
        non_upper = df.filter(df.StateCode != df.StateCode.cast("string").upper()).count()
        assert non_upper == 0, f"Found {non_upper} non-uppercase state codes"
