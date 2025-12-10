"""
Comprehensive test suite for 0_calendar.py module.

Tests cover:
- Calendar DataFrame creation
- Date range validation
- Column presence and types
- Data integrity
- Edge cases
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import tempfile
import shutil
import os
from datetime import datetime, date

import sys
import os

# Get the directory where this test file is located
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TEST_DIR)

from utils import get_spark
import importlib.util

# Load the calendar module from the same directory
spec = importlib.util.spec_from_file_location("calendar_module", os.path.join(TEST_DIR, "0_calendar.py"))
calendar_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(calendar_module)


class TestCalendarDataFrame:
    """Test suite for create_calendar_dataframe function."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture to provide a SparkSession."""
        spark = get_spark("test_calendar")
        yield spark
        spark.stop()
    
    def test_create_calendar_dataframe_success(self, spark):
        """Test successful calendar DataFrame creation."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        assert calendar_df is not None
        assert calendar_df.count() > 0
    
    def test_calendar_has_required_columns(self, spark):
        """Test that calendar DataFrame has all required columns."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        expected_columns = [
            "CalendarID", "CalendarDate", "Year", "Quarter", "Month", "MonthName",
            "Day", "DayOfWeek", "DayName", "WeekOfYear", "ISOYear", "ISOWeek",
            "IsWeekend", "IsHoliday", "HolidayName", "IsMonthStart", "IsMonthEnd",
            "IsQuarterStart", "IsQuarterEnd", "IsYearStart", "IsYearEnd",
            "FiscalYear", "FiscalMonth", "FiscalQuarter", "DayOfYear", "CreatedDate"
        ]
        
        actual_columns = calendar_df.columns
        assert set(expected_columns) == set(actual_columns)
    
    def test_calendar_date_range(self, spark):
        """Test that calendar covers expected date range."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # Get min and max dates
        min_max = calendar_df.selectExpr("min(CalendarDate) as min_date", 
                                         "max(CalendarDate) as max_date").collect()[0]
        
        assert str(min_max.min_date) == "1980-01-01"
        assert str(min_max.max_date) == "2080-12-31"
    
    def test_calendar_id_is_sequential(self, spark):
        """Test that CalendarID is sequential starting from 1."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # Check first few IDs
        first_rows = calendar_df.orderBy("CalendarID").limit(5).collect()
        for i, row in enumerate(first_rows, 1):
            assert row.CalendarID == i
    
    def test_calendar_no_duplicate_dates(self, spark):
        """Test that there are no duplicate dates."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        total_count = calendar_df.count()
        distinct_count = calendar_df.select("CalendarDate").distinct().count()
        
        assert total_count == distinct_count
    
    def test_weekend_flag_saturday_sunday(self, spark):
        """Test that IsWeekend flag is correct for Saturdays and Sundays."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # Check that DayOfWeek 6 and 7 have IsWeekend = 1
        weekend_df = calendar_df.filter("DayOfWeek IN (6, 7)")
        weekend_count = weekend_df.filter("IsWeekend = 1").count()
        total_weekend = weekend_df.count()
        
        assert weekend_count == total_weekend
    
    def test_weekday_flag(self, spark):
        """Test that IsWeekend flag is 0 for weekdays."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # Check that DayOfWeek 1-5 have IsWeekend = 0
        weekday_df = calendar_df.filter("DayOfWeek IN (1, 2, 3, 4, 5)")
        weekday_count = weekday_df.filter("IsWeekend = 0").count()
        total_weekday = weekday_df.count()
        
        assert weekday_count == total_weekday
    
    def test_month_start_flag(self, spark):
        """Test that IsMonthStart flag is correct."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # All month starts should have Day = 1
        month_start_df = calendar_df.filter("IsMonthStart = 1")
        assert month_start_df.filter("Day != 1").count() == 0
    
    def test_year_start_flag(self, spark):
        """Test that IsYearStart flag is correct."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # All year starts should be January 1
        year_start_df = calendar_df.filter("IsYearStart = 1")
        assert year_start_df.filter("Month != 1 OR Day != 1").count() == 0
    
    def test_year_end_flag(self, spark):
        """Test that IsYearEnd flag is correct."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # All year ends should be December 31
        year_end_df = calendar_df.filter("IsYearEnd = 1")
        assert year_end_df.filter("Month != 12 OR Day != 31").count() == 0
    
    def test_quarter_values_valid(self, spark):
        """Test that Quarter column has valid values (1-4)."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        invalid_quarters = calendar_df.filter("Quarter NOT IN (1, 2, 3, 4)").count()
        assert invalid_quarters == 0
    
    def test_month_values_valid(self, spark):
        """Test that Month column has valid values (1-12)."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        invalid_months = calendar_df.filter("Month < 1 OR Month > 12").count()
        assert invalid_months == 0
    
    def test_day_of_week_values_valid(self, spark):
        """Test that DayOfWeek column has valid values (1-7)."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        invalid_dow = calendar_df.filter("DayOfWeek < 1 OR DayOfWeek > 7").count()
        assert invalid_dow == 0
    
    def test_fiscal_year_offset(self, spark):
        """Test that fiscal year is calculated correctly (Q2 start)."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # For dates in Jan-Mar, fiscal year should be same as calendar year
        # For dates in Apr-Dec, fiscal year should be calendar year + 1
        jan_row = calendar_df.filter("Month = 1").limit(1).collect()[0]
        apr_row = calendar_df.filter("Month = 4").limit(1).collect()[0]
        
        assert jan_row.FiscalYear == jan_row.Year
        assert apr_row.FiscalYear == apr_row.Year + 1
    
    def test_none_spark_raises_error(self):
        """Test that None SparkSession raises ValueError."""
        with pytest.raises(ValueError, match="SparkSession cannot be None"):
            calendar_module.create_calendar_dataframe(None)


class TestCalendarMain:
    """Test suite for calendar main function."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        """Fixture to provide a temporary directory."""
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_main_creates_parquet_file(self, temp_dir):
        """Test that main function creates parquet file."""
        calendar_module.main(temp_dir)
        
        output_path = os.path.join(temp_dir, "dim.Calendar")
        assert os.path.exists(output_path)
    
    def test_main_creates_partitioned_data(self, temp_dir):
        """Test that data is partitioned by Year."""
        calendar_module.main(temp_dir)
        
        output_path = os.path.join(temp_dir, "dim.Calendar")
        
        # Check for year partitions
        year_dirs = [d for d in os.listdir(output_path) if d.startswith("Year=")]
        assert len(year_dirs) > 0
    
    def test_main_with_invalid_output_root(self):
        """Test that invalid output root raises ValueError."""
        with pytest.raises(ValueError, match="output_root must be a non-empty string"):
            calendar_module.main("")
    
    def test_main_data_integrity(self, temp_dir):
        """Test data integrity after write and read."""
        calendar_module.main(temp_dir)
        
        spark = get_spark("test_read")
        output_path = os.path.join(temp_dir, "dim.Calendar")
        read_df = spark.read.parquet(output_path)
        
        # Verify record count
        assert read_df.count() > 36000  # ~100 years * 365 days
        
        # Verify no nulls in key columns
        assert read_df.filter("CalendarID IS NULL").count() == 0
        assert read_df.filter("CalendarDate IS NULL").count() == 0
        assert read_df.filter("Year IS NULL").count() == 0
        
        spark.stop()


class TestEdgeCases:
    """Test edge cases and boundary conditions."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Fixture to provide a SparkSession."""
        spark = get_spark("test_edge_cases")
        yield spark
        spark.stop()
    
    def test_leap_year_handling(self, spark):
        """Test that leap years are handled correctly."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # 2000 was a leap year, should have Feb 29
        leap_day_2000 = calendar_df.filter("Year = 2000 AND Month = 2 AND Day = 29").count()
        assert leap_day_2000 == 1
        
        # 2001 was not a leap year, should not have Feb 29
        leap_day_2001 = calendar_df.filter("Year = 2001 AND Month = 2 AND Day = 29").count()
        assert leap_day_2001 == 0
    
    def test_century_leap_year(self, spark):
        """Test century leap year rules (2000 was leap, 1900 wasn't)."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # 2000 was a leap year (divisible by 400)
        leap_2000 = calendar_df.filter("Year = 2000 AND Month = 2 AND Day = 29").count()
        assert leap_2000 == 1
    
    def test_all_months_have_correct_days(self, spark):
        """Test that all months have the correct number of days."""
        calendar_df = calendar_module.create_calendar_dataframe(spark)
        
        # January should have 31 days in all years
        jan_days = calendar_df.filter("Year = 2020 AND Month = 1").count()
        assert jan_days == 31
        
        # February in non-leap year should have 28 days
        feb_days = calendar_df.filter("Year = 2021 AND Month = 2").count()
        assert feb_days == 28


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
