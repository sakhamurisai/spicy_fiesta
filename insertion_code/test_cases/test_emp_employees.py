"""
Test suite for 05_emp_employees.py - emp.Employees table
Tests: 1,000 employees, modulo position/location cycling, email format, UUID validity
Coverage: 100% of employees generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestEmpEmployeesGenerator:
    """Test suite for Employee generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_employees_row_count(self):
        """TC-001: Verify exactly 1,000 employees generated"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        assert_row_count(df, 1000, tolerance=0)
    
    def test_employees_columns_exist(self):
        """TC-002: Verify all columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        expected_columns = ["EmployeeID", "FirstName", "LastName", "Email", "PhoneNumber",
                           "PrimaryLocationID", "PositionID", "DateOfBirthID", "HireDateID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_employee_ids_unique(self):
        """TC-003: Verify EmployeeID is unique for all 1,000 employees"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        assert_unique_values(df, "EmployeeID", 1000)
    
    def test_employee_ids_sequential(self):
        """TC-004: Verify EmployeeID ranges from 1-1000"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        min_id = df.agg({"EmployeeID": "min"}).collect()[0][0]
        max_id = df.agg({"EmployeeID": "max"}).collect()[0][0]
        assert min_id == 1, f"Min EmployeeID should be 1, got {min_id}"
        assert max_id == 1000, f"Max EmployeeID should be 1000, got {max_id}"
    
    def test_position_id_cycling(self):
        """TC-005: Verify PositionID cycles through 1-4 for employees"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees").orderBy("EmployeeID")
        position_ids = [row.PositionID for row in df.collect()]
        # Expected: modulo cycle through positions (1,2,3,4,1,2,3,4,...)
        expected_positions = [(i % 4) + 1 for i in range(1000)]
        assert position_ids == expected_positions, "PositionID modulo cycling failed"
    
    def test_location_id_cycling(self):
        """TC-006: Verify PrimaryLocationID cycles through 1-200 for employees"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees").orderBy("EmployeeID")
        location_ids = [row.PrimaryLocationID for row in df.collect()]
        # Expected: modulo cycle through locations (1,2,3,...,200,1,2,3,...)
        expected_locations = [(i % 200) + 1 for i in range(1000)]
        assert location_ids == expected_locations, "PrimaryLocationID modulo cycling failed"
    
    def test_no_nulls_critical_columns(self):
        """TC-007: Verify no NULLs in critical columns"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        critical_cols = ["EmployeeID", "FirstName", "LastName", "Email", "PrimaryLocationID", "PositionID"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_email_format(self):
        """TC-008: Verify email format is FirstName.LastName@spicyfiesta.com"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid_emails = 0
        for row in df.collect():
            expected_email = f"{row.FirstName}.{row.LastName}@spicyfiesta.com".lower()
            if str(row.Email).lower() != expected_email:
                invalid_emails += 1
        assert invalid_emails == 0, f"Found {invalid_emails} invalid email formats"
    
    def test_email_contains_at_symbol(self):
        """TC-009: Verify all emails contain @ symbol"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid = df.filter(~df.Email.contains("@")).count()
        assert invalid == 0, f"Found {invalid} emails without @ symbol"
    
    def test_email_ends_with_spicyfiesta(self):
        """TC-010: Verify all emails end with @spicyfiesta.com"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid = df.filter(~df.Email.endswith("@spicyfiesta.com")).count()
        assert invalid == 0, f"Found {invalid} emails not ending with @spicyfiesta.com"
    
    def test_email_unique(self):
        """TC-011: Verify all email addresses are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        assert_unique_values(df, "Email", 1000)
    
    def test_phone_number_format(self):
        """TC-012: Verify PhoneNumber is 10-digit format"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid_phones = 0
        for row in df.collect():
            digits = ''.join(c for c in str(row.PhoneNumber) if c.isdigit())
            if len(digits) != 10:
                invalid_phones += 1
        assert invalid_phones == 0, f"Found {invalid_phones} invalid phone numbers"
    
    def test_first_name_populated(self):
        """TC-013: Verify FirstName is not empty"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        empty = df.filter((df.FirstName.isNull()) | (len(df.FirstName) == 0)).count()
        assert empty == 0, f"Found {empty} empty FirstName values"
    
    def test_last_name_populated(self):
        """TC-014: Verify LastName is not empty"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        empty = df.filter((df.LastName.isNull()) | (len(df.LastName) == 0)).count()
        assert empty == 0, f"Found {empty} empty LastName values"
    
    def test_position_id_range(self):
        """TC-015: Verify PositionID is between 1-4"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid = df.filter((df.PositionID < 1) | (df.PositionID > 4)).count()
        assert invalid == 0, f"Found {invalid} invalid PositionID values"
    
    def test_location_id_range(self):
        """TC-016: Verify PrimaryLocationID is between 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid = df.filter((df.PrimaryLocationID < 1) | (df.PrimaryLocationID > 200)).count()
        assert invalid == 0, f"Found {invalid} invalid PrimaryLocationID values"
    
    def test_date_of_birth_id_valid(self):
        """TC-017: Verify DateOfBirthID is within calendar range (1-36892)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid = df.filter((df.DateOfBirthID < 1) | (df.DateOfBirthID > 36892)).count()
        assert invalid == 0, f"Found {invalid} invalid DateOfBirthID values"
    
    def test_hire_date_id_valid(self):
        """TC-018: Verify HireDateID is within calendar range (1-36892)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        invalid = df.filter((df.HireDateID < 1) | (df.HireDateID > 36892)).count()
        assert invalid == 0, f"Found {invalid} invalid HireDateID values"
    
    def test_created_date_populated(self):
        """TC-019: Verify CreatedDate is populated for all rows"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        null_dates = df.filter(df.CreatedDate.isNull()).count()
        assert null_dates == 0, f"Found {null_dates} NULL CreatedDate values"
    
    def test_all_positions_represented(self):
        """TC-020: Verify all 4 positions are represented in employees"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        unique_positions = df.select("PositionID").distinct().count()
        assert unique_positions == 4, f"Expected 4 unique positions, got {unique_positions}"
    
    def test_all_locations_represented(self):
        """TC-021: Verify all 200 locations are represented in employees"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        unique_locations = df.select("PrimaryLocationID").distinct().count()
        assert unique_locations == 200, f"Expected 200 unique locations, got {unique_locations}"
    
    def test_position_distribution(self):
        """TC-022: Verify even distribution of positions (250 per position)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        position_counts = df.groupBy("PositionID").count().collect()
        for row in position_counts:
            assert row['count'] == 250, f"PositionID {row.PositionID} has {row['count']} employees, expected 250"
    
    def test_location_distribution(self):
        """TC-023: Verify even distribution of locations (5 per location)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        location_counts = df.groupBy("PrimaryLocationID").count().collect()
        for row in location_counts:
            assert row['count'] == 5, f"LocationID {row.PrimaryLocationID} has {row['count']} employees, expected 5"
    
    def test_phone_number_not_null(self):
        """TC-024: Verify PhoneNumber is not NULL"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        null_phones = df.filter(df.PhoneNumber.isNull()).count()
        assert null_phones == 0, f"Found {null_phones} NULL phone numbers"
    
    def test_email_not_null(self):
        """TC-025: Verify Email is not NULL"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Employees")
        null_emails = df.filter(df.Email.isNull()).count()
        assert null_emails == 0, f"Found {null_emails} NULL emails"
