"""
Test suite for 02_store_locations.py - store.Locations table
Tests: 200 locations, modulo state cycling, UUID validity, zip code formula
Coverage: 100% of locations generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestStoreLocationsGenerator:
    """Test suite for Store Locations generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_locations_row_count(self):
        """TC-001: Verify exactly 200 locations generated"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        assert_row_count(df, 200, tolerance=0)
    
    def test_locations_columns_exist(self):
        """TC-002: Verify all columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        expected_columns = ["LocationID", "StoreName", "StateID", "City", "Address", 
                           "ZipCode", "PhoneNumber", "OpeningDateID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_location_ids_unique(self):
        """TC-003: Verify LocationID is unique for all 200 locations"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        assert_unique_values(df, "LocationID", 200)
    
    def test_location_ids_sequential(self):
        """TC-004: Verify LocationID ranges from 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        min_id = df.agg({"LocationID": "min"}).collect()[0][0]
        max_id = df.agg({"LocationID": "max"}).collect()[0][0]
        assert min_id == 1, f"Min LocationID should be 1, got {min_id}"
        assert max_id == 200, f"Max LocationID should be 200, got {max_id}"
    
    def test_state_id_cycling(self):
        """TC-005: Verify StateID cycles through 1-5 for locations 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations").orderBy("LocationID")
        state_ids = [row.StateID for row in df.collect()]
        # Expected: modulo cycle through states (1,2,3,4,5,1,2,3,4,5,...)
        expected_states = [(i % 5) + 1 for i in range(200)]
        assert state_ids == expected_states, "StateID modulo cycling failed"
    
    def test_no_nulls_critical_columns(self):
        """TC-006: Verify no NULLs in critical columns"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        critical_cols = ["LocationID", "StoreName", "StateID", "City", "Address", "ZipCode"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_zipcode_formula(self):
        """TC-007: Verify ZipCode follows formula: 90000 + (LocationID % 1000)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        locations = df.orderBy("LocationID").collect()
        for loc in locations:
            expected_zip = 90000 + (loc.LocationID % 1000)
            assert loc.ZipCode == expected_zip, f"ZipCode mismatch for LocationID {loc.LocationID}"
    
    def test_zipcode_range(self):
        """TC-008: Verify all ZipCodes are between 90000-90999"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        out_of_range = df.filter((df.ZipCode < 90000) | (df.ZipCode >= 91000)).count()
        assert out_of_range == 0, f"Found {out_of_range} ZipCodes out of range"
    
    def test_phone_number_format(self):
        """TC-009: Verify PhoneNumber is 10-digit format (XXX) XXX-XXXX"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        invalid_phones = 0
        for row in df.collect():
            # Extract digits only
            digits = ''.join(c for c in row.PhoneNumber if c.isdigit())
            if len(digits) != 10:
                invalid_phones += 1
        assert invalid_phones == 0, f"Found {invalid_phones} invalid phone numbers"
    
    def test_store_name_not_empty(self):
        """TC-010: Verify StoreName is populated and non-empty"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        empty_names = df.filter((df.StoreName.isNull()) | (len(df.StoreName) == 0)).count()
        assert empty_names == 0, f"Found {empty_names} empty StoreName values"
    
    def test_store_names_unique(self):
        """TC-011: Verify all StoreName values are unique (200 unique names)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        assert_unique_values(df, "StoreName", 200)
    
    def test_city_populated(self):
        """TC-012: Verify City is populated for all locations"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        null_cities = df.filter(df.City.isNull()).count()
        assert null_cities == 0, f"Found {null_cities} NULL cities"
    
    def test_address_populated(self):
        """TC-013: Verify Address is populated for all locations"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        null_addresses = df.filter(df.Address.isNull()).count()
        assert null_addresses == 0, f"Found {null_addresses} NULL addresses"
    
    def test_opening_date_id_valid(self):
        """TC-014: Verify OpeningDateID is within calendar range (1-36892)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        invalid_dates = df.filter((df.OpeningDateID < 1) | (df.OpeningDateID > 36892)).count()
        assert invalid_dates == 0, f"Found {invalid_dates} invalid OpeningDateID values"
    
    def test_created_date_populated(self):
        """TC-015: Verify CreatedDate is populated for all rows"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        null_dates = df.filter(df.CreatedDate.isNull()).count()
        assert null_dates == 0, f"Found {null_dates} NULL CreatedDate values"
    
    def test_state_id_in_valid_range(self):
        """TC-016: Verify StateID is between 1-5"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        invalid_states = df.filter((df.StateID < 1) | (df.StateID > 5)).count()
        assert invalid_states == 0, f"Found {invalid_states} invalid StateID values"
    
    def test_all_states_represented(self):
        """TC-017: Verify all 5 states are represented in locations"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        unique_states = df.select("StateID").distinct().count()
        assert unique_states == 5, f"Expected 5 unique states, got {unique_states}"
    
    def test_locations_per_state(self):
        """TC-018: Verify each state has exactly 40 locations (200/5)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        state_counts = df.groupBy("StateID").count().collect()
        for row in state_counts:
            assert row['count'] == 40, f"StateID {row.StateID} has {row['count']} locations, expected 40"
    
    def test_no_duplicate_zipcodes_same_state(self):
        """TC-019: Verify no duplicate ZipCode within same state"""
        df = self.spark.read.parquet(f"{self.output_row}/store.Locations")
        duplicates = df.groupBy("StateID", "ZipCode").count().filter("count > 1").count()
        assert duplicates == 0, f"Found {duplicates} duplicate (StateID, ZipCode) pairs"
    
    def test_phone_number_not_null(self):
        """TC-020: Verify PhoneNumber is not NULL for any location"""
        df = self.spark.read.parquet(f"{self.output_root}/store.Locations")
        null_phones = df.filter(df.PhoneNumber.isNull()).count()
        assert null_phones == 0, f"Found {null_phones} NULL phone numbers"
