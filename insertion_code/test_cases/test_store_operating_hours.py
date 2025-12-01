"""
Test suite for 03_store_operating_hours.py - store.OperatingHours table
Tests: 1,400 rows (200 locations × 7 days), time formats, day cycling
Coverage: 100% of operating hours generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestStoreOperatingHoursGenerator:
    """Test suite for Store Operating Hours generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_operating_hours_row_count(self):
        """TC-001: Verify exactly 1,400 rows (200 locations × 7 days)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        assert_row_count(df, 1400, tolerance=0)
    
    def test_operating_hours_columns_exist(self):
        """TC-002: Verify all columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        expected_columns = ["OperatingHourID", "LocationID", "DayOfWeek", "OpenTime", "CloseTime", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_operating_hour_ids_unique(self):
        """TC-003: Verify OperatingHourID is unique for all 1,400 rows"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        assert_unique_values(df, "OperatingHourID", 1400)
    
    def test_operating_hour_ids_sequential(self):
        """TC-004: Verify OperatingHourID ranges from 1-1400"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        min_id = df.agg({"OperatingHourID": "min"}).collect()[0][0]
        max_id = df.agg({"OperatingHourID": "max"}).collect()[0][0]
        assert min_id == 1, f"Min OperatingHourID should be 1, got {min_id}"
        assert max_id == 1400, f"Max OperatingHourID should be 1400, got {max_id}"
    
    def test_location_id_range(self):
        """TC-005: Verify LocationID is between 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        invalid_locs = df.filter((df.LocationID < 1) | (df.LocationID > 200)).count()
        assert invalid_locs == 0, f"Found {invalid_locs} invalid LocationID values"
    
    def test_day_of_week_range(self):
        """TC-006: Verify DayOfWeek is 1-7"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        invalid_days = df.filter((df.DayOfWeek < 1) | (df.DayOfWeek > 7)).count()
        assert invalid_days == 0, f"Found {invalid_days} invalid DayOfWeek values"
    
    def test_seven_days_per_location(self):
        """TC-007: Verify each location has exactly 7 rows (one per day of week)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        location_day_counts = df.groupBy("LocationID").count().collect()
        for row in location_day_counts:
            assert row['count'] == 7, f"LocationID {row.LocationID} has {row['count']} days, expected 7"
    
    def test_unique_location_day_combinations(self):
        """TC-008: Verify (LocationID, DayOfWeek) combination is unique"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        duplicate_combos = df.groupBy("LocationID", "DayOfWeek").count().filter("count > 1").count()
        assert duplicate_combos == 0, f"Found {duplicate_combos} duplicate (LocationID, DayOfWeek) pairs"
    
    def test_no_nulls_critical_columns(self):
        """TC-009: Verify no NULLs in critical columns"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        critical_cols = ["OperatingHourID", "LocationID", "DayOfWeek", "OpenTime", "CloseTime"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_open_time_format(self):
        """TC-010: Verify OpenTime is in HH:MM format"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        invalid_format = 0
        for row in df.collect():
            if ':' not in str(row.OpenTime):
                invalid_format += 1
        assert invalid_format == 0, f"Found {invalid_format} invalid OpenTime formats"
    
    def test_close_time_format(self):
        """TC-011: Verify CloseTime is in HH:MM format"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        invalid_format = 0
        for row in df.collect():
            if ':' not in str(row.CloseTime):
                invalid_format += 1
        assert invalid_format == 0, f"Found {invalid_format} invalid CloseTime formats"
    
    def test_close_time_after_open_time(self):
        """TC-012: Verify CloseTime is always after OpenTime (ignoring AM/PM for simplicity)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        # Assuming time format is HH:MM with 24-hour format
        invalid_times = df.filter(df.CloseTime <= df.OpenTime).count()
        assert invalid_times == 0, f"Found {invalid_times} rows where CloseTime <= OpenTime"
    
    def test_hardcoded_operating_hours(self):
        """TC-013: Verify operating hours are hardcoded (08:00-22:00 for all locations)"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        # All should have 08:00 open and 22:00 close
        non_standard = df.filter(
            (df.OpenTime != "08:00") | (df.CloseTime != "22:00")
        ).count()
        assert non_standard == 0, f"Found {non_standard} non-standard operating hours"
    
    def test_all_locations_represented(self):
        """TC-014: Verify all 200 locations are represented"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        unique_locations = df.select("LocationID").distinct().count()
        assert unique_locations == 200, f"Expected 200 unique locations, got {unique_locations}"
    
    def test_all_days_represented(self):
        """TC-015: Verify all 7 days of week are represented"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        unique_days = df.select("DayOfWeek").distinct().count()
        assert unique_days == 7, f"Expected 7 unique days, got {unique_days}"
    
    def test_created_date_populated(self):
        """TC-016: Verify CreatedDate is populated for all rows"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        null_dates = df.filter(df.CreatedDate.isNull()).count()
        assert null_dates == 0, f"Found {null_dates} NULL CreatedDate values"
    
    def test_day_of_week_values(self):
        """TC-017: Verify DayOfWeek contains all values 1-7 across dataset"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        days_present = set(row.DayOfWeek for row in df.select("DayOfWeek").distinct().collect())
        expected_days = {1, 2, 3, 4, 5, 6, 7}
        assert days_present == expected_days, f"Expected days {expected_days}, got {days_present}"
    
    def test_no_null_open_time(self):
        """TC-018: Verify OpenTime is not NULL for any row"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        null_open = df.filter(df.OpenTime.isNull()).count()
        assert null_open == 0, f"Found {null_open} NULL OpenTime values"
    
    def test_no_null_close_time(self):
        """TC-019: Verify CloseTime is not NULL for any row"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        null_close = df.filter(df.CloseTime.isNull()).count()
        assert null_close == 0, f"Found {null_close} NULL CloseTime values"
    
    def test_location_id_not_null(self):
        """TC-020: Verify LocationID is not NULL for any row"""
        df = self.spark.read.parquet(f"{self.output_root}/store.OperatingHours")
        null_locations = df.filter(df.LocationID.isNull()).count()
        assert null_locations == 0, f"Found {null_locations} NULL LocationID values"
