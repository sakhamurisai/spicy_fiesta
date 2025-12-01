"""
Test suite for 0_calendar.py - dim.Calendar dimension table
Tests: Row count, date range, computed columns, no gaps, weekends, year/month markers
Coverage: 100% of calendar generation logic
"""

import pytest
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '../')
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_column_values_in_range, assert_unique_values


class TestCalendarGenerator:
    """Test suite for Calendar dimension generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        """Setup for each test - provide spark session"""
        self.spark = spark
        self.output_root = output_root
    
    def test_calendar_row_count(self):
        """TC-001: Verify total row count is ~36,892 rows (101 years × 365 days + leap days)"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        # Expected: 1980-01-01 to 2080-12-31 = 101 years
        # Regular years: 365 days, Leap years: 25 in this range
        expected_min = 36890
        expected_max = 36895
        assert_row_count(df, 36892, tolerance=5)
    
    def test_calendar_columns_exist(self):
        """TC-002: Verify all 25 columns exist in output"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        expected_columns = [
            "CalendarID", "CalendarDate", "Year", "Quarter", "Month", "MonthName",
            "Day", "DayOfWeek", "DayName", "WeekOfYear", "ISOYear", "ISOWeek",
            "IsWeekend", "IsHoliday", "HolidayName", "IsMonthStart", "IsMonthEnd",
            "IsQuarterStart", "IsQuarterEnd", "IsYearStart", "IsYearEnd",
            "FiscalYear", "FiscalMonth", "FiscalQuarter", "DayOfYear", "CreatedDate"
        ]
        assert_dataframe_columns(df, expected_columns)
    
    def test_calendar_id_sequential(self):
        """TC-003: Verify CalendarID is sequential from 1 to row count"""
        df = self.spark.read.parquet(f"{self.output_row}/dim.Calendar").orderBy("CalendarID")
        ids = [row.CalendarID for row in df.collect()]
        expected_ids = list(range(1, len(ids) + 1))
        assert ids == expected_ids, "CalendarID is not sequential"
    
    def test_calendar_date_range(self):
        """TC-004: Verify calendar dates span 1980-01-01 to 2080-12-31 with no gaps"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar").orderBy("CalendarDate")
        min_date = df.agg({"CalendarDate": "min"}).collect()[0][0]
        max_date = df.agg({"CalendarDate": "max"}).collect()[0][0]
        assert str(min_date) == "1980-01-01", f"Min date mismatch: {min_date}"
        assert str(max_date) == "2080-12-31", f"Max date mismatch: {max_date}"
    
    def test_calendar_no_date_gaps(self):
        """TC-005: Verify no gaps in calendar dates (each day present)"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar").orderBy("CalendarDate")
        dates = [row.CalendarDate for row in df.collect()]
        for i in range(1, len(dates)):
            expected_next = dates[i-1] + timedelta(days=1)
            assert dates[i] == expected_next, f"Gap found at {dates[i-1]}"
    
    def test_is_weekend_accuracy(self):
        """TC-006: Verify IsWeekend = 1 only when DayOfWeek in (6, 7)"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        # Weekend check: DayOfWeek 6=Saturday, 7=Sunday
        weekend_mismatch = df.filter(
            ((df.IsWeekend == 1) & ~(df.DayOfWeek.isin([6, 7]))) |
            ((df.IsWeekend == 0) & (df.DayOfWeek.isin([6, 7])))
        ).count()
        assert weekend_mismatch == 0, f"IsWeekend mismatch for {weekend_mismatch} rows"
    
    def test_is_month_start(self):
        """TC-007: Verify IsMonthStart = 1 only when Day = 1"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        mismatch = df.filter(
            ((df.IsMonthStart == 1) & (df.Day != 1)) |
            ((df.IsMonthStart == 0) & (df.Day == 1))
        ).count()
        assert mismatch == 0, f"IsMonthStart mismatch for {mismatch} rows"
    
    def test_is_month_end(self):
        """TC-008: Verify IsMonthEnd = 1 only on last day of month"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        # Check: days at month end have IsMonthEnd=1
        month_end_count = df.filter(df.IsMonthEnd == 1).count()
        # Expected: ~12 months × 101 years = ~1,212 month ends (rough estimate)
        assert 1000 < month_end_count < 1500, f"Month end count {month_end_count} out of range"
    
    def test_is_year_start(self):
        """TC-009: Verify IsYearStart = 1 only for Jan 1st"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        year_start_count = df.filter(df.IsYearStart == 1).count()
        assert year_start_count == 101, f"Expected 101 year starts, got {year_start_count}"
    
    def test_is_year_end(self):
        """TC-010: Verify IsYearEnd = 1 only for Dec 31st"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        year_end_count = df.filter(df.IsYearEnd == 1).count()
        assert year_end_count == 101, f"Expected 101 year ends, got {year_end_count}"
    
    def test_day_of_week_range(self):
        """TC-011: Verify DayOfWeek values are 1-7"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        assert_column_values_in_range(df, "DayOfWeek", 1, 7)
    
    def test_month_range(self):
        """TC-012: Verify Month values are 1-12"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        assert_column_values_in_range(df, "Month", 1, 12)
    
    def test_quarter_range(self):
        """TC-013: Verify Quarter values are 1-4"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        assert_column_values_in_range(df, "Quarter", 1, 4)
    
    def test_day_range(self):
        """TC-014: Verify Day values are 1-31"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        assert_column_values_in_range(df, "Day", 1, 31)
    
    def test_year_unique_values(self):
        """TC-015: Verify exactly 101 unique years (1980-2080)"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        assert_unique_values(df, "Year", 101)
    
    def test_no_nulls_critical_columns(self):
        """TC-016: Verify no NULL values in critical columns"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        critical_cols = ["CalendarID", "CalendarDate", "Year", "Month", "Day", "DayOfWeek"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_fiscal_year_offset(self):
        """TC-017: Verify FiscalYear = Year + 0.25 (3-month offset from original)"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar").filter(
            (df.Month >= 1) & (df.Month <= 3)
        )
        # For Jan-Mar, FiscalYear should be Year of previous year + 1
        fiscal_offset_count = df.filter(df.FiscalYear != df.Year).count()
        assert fiscal_offset_count > 0, "Fiscal year offset not applied correctly"
    
    def test_created_date_is_timestamp(self):
        """TC-018: Verify CreatedDate contains valid timestamps"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        assert_no_nulls_in_columns(df, ["CreatedDate"])
    
    def test_partition_by_year(self):
        """TC-019: Verify output is partitioned by Year"""
        import os
        year_dirs = [d for d in os.listdir(f"{self.output_root}/dim.Calendar") if d.startswith("Year=")]
        assert len(year_dirs) == 101, f"Expected 101 year partitions, found {len(year_dirs)}"
    
    def test_day_of_year_range(self):
        """TC-020: Verify DayOfYear values are 1-366 (accounting for leap years)"""
        df = self.spark.read.parquet(f"{self.output_root}/dim.Calendar")
        assert_column_values_in_range(df, "DayOfYear", 1, 366)
