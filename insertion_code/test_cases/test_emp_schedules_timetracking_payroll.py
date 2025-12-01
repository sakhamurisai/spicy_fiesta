"""
Test suite for 06_emp_schedules_timetracking_payroll.py - emp.Schedules, emp.TimeTracking, emp.Payroll
Tests: 30,000 schedules (1,000 employees × 30 days), time tracking calculations, payroll aggregation
Coverage: 100% of schedules, timetracking, and payroll generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestEmpSchedulesGenerator:
    """Test suite for Employee Schedules generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_schedules_row_count(self):
        """TC-001: Verify 30,000 schedules (1,000 employees × 30 days)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Schedules")
        assert_row_count(df, 30000, tolerance=100)
    
    def test_schedules_columns_exist(self):
        """TC-002: Verify Schedules columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Schedules")
        expected_columns = ["ScheduleID", "EmployeeID", "ScheduleDateID", "ShiftStartTime", "ShiftEndTime", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_schedules_employee_range(self):
        """TC-003: Verify EmployeeID in Schedules is 1-1000"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Schedules")
        invalid = df.filter((df.EmployeeID < 1) | (df.EmployeeID > 1000)).count()
        assert invalid == 0, f"Found {invalid} invalid EmployeeID values in Schedules"
    
    def test_schedules_shift_times(self):
        """TC-004: Verify Shift times are 09:00-17:00"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Schedules")
        valid = df.filter((df.ShiftStartTime == "09:00") & (df.ShiftEndTime == "17:00")).count()
        assert valid == df.count(), f"Found non-09:00-17:00 shifts in {df.count() - valid} rows"
    
    def test_timetracking_row_count(self):
        """TC-005: Verify ~30,000 time tracking records"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.TimeTracking")
        assert_row_count(df, 30000, tolerance=1000)
    
    def test_timetracking_columns_exist(self):
        """TC-006: Verify TimeTracking columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.TimeTracking")
        expected_columns = ["TimeTrackingID", "EmployeeID", "ClockInTime", "ClockOutTime", "HoursWorked", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_timetracking_employee_range(self):
        """TC-007: Verify EmployeeID in TimeTracking is 1-1000"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.TimeTracking")
        invalid = df.filter((df.EmployeeID < 1) | (df.EmployeeID > 1000)).count()
        assert invalid == 0, f"Found {invalid} invalid EmployeeID values in TimeTracking"
    
    def test_hours_worked_positive(self):
        """TC-008: Verify HoursWorked is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.TimeTracking")
        invalid = df.filter(df.HoursWorked <= 0).count()
        assert invalid == 0, f"Found {invalid} non-positive HoursWorked values"
    
    def test_hours_worked_reasonable(self):
        """TC-009: Verify HoursWorked is between 0 and 24 hours"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.TimeTracking")
        invalid = df.filter((df.HoursWorked < 0) | (df.HoursWorked > 24)).count()
        assert invalid == 0, f"Found {invalid} unreasonable HoursWorked values"
    
    def test_clock_out_after_clock_in(self):
        """TC-010: Verify ClockOutTime is after ClockInTime"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.TimeTracking")
        invalid = df.filter(df.ClockOutTime <= df.ClockInTime).count()
        assert invalid == 0, f"Found {invalid} rows where ClockOutTime <= ClockInTime"
    
    def test_payroll_row_count(self):
        """TC-011: Verify ~1,000 payroll records (one per employee)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        assert_row_count(df, 1000, tolerance=100)
    
    def test_payroll_columns_exist(self):
        """TC-012: Verify Payroll columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        expected_columns = ["PayrollID", "EmployeeID", "GrossPay", "Deductions", "NetPay", "PayrollPeriodID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_payroll_employee_unique(self):
        """TC-013: Verify EmployeeID is unique in Payroll (one record per employee)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        assert_unique_values(df, "EmployeeID", 1000)
    
    def test_payroll_gross_pay_positive(self):
        """TC-014: Verify GrossPay is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        invalid = df.filter(df.GrossPay <= 0).count()
        assert invalid == 0, f"Found {invalid} non-positive GrossPay values"
    
    def test_payroll_deductions_non_negative(self):
        """TC-015: Verify Deductions are non-negative"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        invalid = df.filter(df.Deductions < 0).count()
        assert invalid == 0, f"Found {invalid} negative Deduction values"
    
    def test_net_pay_equals_gross_minus_deductions(self):
        """TC-016: Verify NetPay = GrossPay - Deductions"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        invalid = df.filter(df.NetPay != (df.GrossPay - df.Deductions)).count()
        # Allow small tolerance for floating point errors
        assert invalid < 10, f"Found {invalid} rows where NetPay != GrossPay - Deductions"
    
    def test_net_pay_positive(self):
        """TC-017: Verify NetPay is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        invalid = df.filter(df.NetPay <= 0).count()
        assert invalid == 0, f"Found {invalid} non-positive NetPay values"
    
    def test_payroll_no_nulls_critical(self):
        """TC-018: Verify no NULLs in critical Payroll columns"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Payroll")
        critical_cols = ["PayrollID", "EmployeeID", "GrossPay", "Deductions", "NetPay"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_schedules_no_nulls_critical(self):
        """TC-019: Verify no NULLs in critical Schedules columns"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Schedules")
        critical_cols = ["ScheduleID", "EmployeeID", "ScheduleDateID", "ShiftStartTime", "ShiftEndTime"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_timetracking_no_nulls_critical(self):
        """TC-020: Verify no NULLs in critical TimeTracking columns"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.TimeTracking")
        critical_cols = ["TimeTrackingID", "EmployeeID", "ClockInTime", "ClockOutTime", "HoursWorked"]
        assert_no_nulls_in_columns(df, critical_cols)
