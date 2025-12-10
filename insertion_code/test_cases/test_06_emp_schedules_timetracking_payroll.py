"""
Comprehensive test suite for 06_emp_schedules_timetracking_payroll.py module.
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

calendar_mod = load_module("calendar", os.path.join(TEST_DIR, "0_calendar.py"))
states_mod = load_module("states", os.path.join(TEST_DIR, "01_store_states.py"))
locations_mod = load_module("locations", os.path.join(TEST_DIR, "02_store_locations.py"))
positions_mod = load_module("positions", os.path.join(TEST_DIR, "04_emp_positions.py"))
employees_mod = load_module("employees", os.path.join(TEST_DIR, "05_emp_employees.py"))
schedules_mod = load_module("schedules", os.path.join(TEST_DIR, "06_emp_schedules_timetracking_payroll.py"))


class TestSchedulesModule:
    """Test suite for schedules, time tracking, and payroll."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        
        # Create prerequisites
        calendar_mod.main(temp_path)
        states_mod.main(temp_path)
        locations_mod.main(temp_path, 3)
        positions_mod.main(temp_path)
        employees_mod.main(temp_path, 5)
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_schedules_created(self, temp_dir):
        """Test that schedules are created."""
        schedules_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        schedules_df = spark.read.parquet(f"{temp_dir}/emp.Schedules")
        assert schedules_df.count() > 0
        spark.stop()
    
    def test_schedules_has_required_columns(self, temp_dir):
        """Test schedules have required columns."""
        schedules_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        schedules_df = spark.read.parquet(f"{temp_dir}/emp.Schedules")
        
        expected_columns = [
            "ScheduleID", "EmployeeID", "LocationID", "ShiftDateID",
            "StartTime", "EndTime", "ShiftType", "BreakMinutes",
            "IsApproved", "ApprovedBy", "CreatedDate"
        ]
        
        assert set(expected_columns).issubset(set(schedules_df.columns))
        spark.stop()
    
    def test_time_tracking_created(self, temp_dir):
        """Test that time tracking is created."""
        schedules_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        tt_df = spark.read.parquet(f"{temp_dir}/emp.TimeTracking")
        assert tt_df.count() > 0
        spark.stop()
    
    def test_payroll_created(self, temp_dir):
        """Test that payroll is created."""
        schedules_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        payroll_df = spark.read.parquet(f"{temp_dir}/emp.Payroll")
        assert payroll_df.count() > 0
        spark.stop()
    
    def test_payroll_hours_positive(self, temp_dir):
        """Test that hours worked are positive."""
        schedules_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        payroll_df = spark.read.parquet(f"{temp_dir}/emp.Payroll")
        
        negative = payroll_df.filter("HoursWorked <= 0").count()
        assert negative == 0
        spark.stop()
    
    def test_payroll_gross_pay_positive(self, temp_dir):
        """Test that gross pay is positive."""
        schedules_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        payroll_df = spark.read.parquet(f"{temp_dir}/emp.Payroll")
        
        negative = payroll_df.filter("GrossPay <= 0").count()
        assert negative == 0
        spark.stop()
    
    def test_time_tracking_has_clock_times(self, temp_dir):
        """Test that time tracking has clock in/out times."""
        schedules_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        tt_df = spark.read.parquet(f"{temp_dir}/emp.TimeTracking")
        
        assert tt_df.filter("ClockInTime IS NULL").count() == 0
        assert tt_df.filter("ClockOutTime IS NULL").count() == 0
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
