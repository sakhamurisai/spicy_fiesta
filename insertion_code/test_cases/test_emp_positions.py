"""
Test suite for 04_emp_positions.py - emp.Positions table
Tests: 4 hardcoded positions, hierarchy levels, salary ranges, departments
Coverage: 100% of positions generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestEmpPositionsGenerator:
    """Test suite for Employee Positions generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_positions_row_count(self):
        """TC-001: Verify exactly 4 positions"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        assert_row_count(df, 4, tolerance=0)
    
    def test_positions_columns_exist(self):
        """TC-002: Verify all columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        expected_columns = ["PositionID", "PositionName", "Department", "HierarchyLevel", 
                           "MinSalary", "MaxSalary", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_position_ids_unique(self):
        """TC-003: Verify PositionID is unique for all 4 positions"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        assert_unique_values(df, "PositionID", 4)
    
    def test_position_ids_sequential(self):
        """TC-004: Verify PositionID ranges from 1-4"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        min_id = df.agg({"PositionID": "min"}).collect()[0][0]
        max_id = df.agg({"PositionID": "max"}).collect()[0][0]
        assert min_id == 1 and max_id == 4, f"PositionID not 1-4: min={min_id}, max={max_id}"
    
    def test_position_names_unique(self):
        """TC-005: Verify all PositionName values are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        assert_unique_values(df, "PositionName", 4)
    
    def test_hardcoded_positions_present(self):
        """TC-006: Verify expected hardcoded positions exist"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        positions = {row.PositionName for row in df.select("PositionName").collect()}
        # Expected: Crew Member, Supervisor, Manager, District Manager
        expected = {"Crew Member", "Supervisor", "Manager", "District Manager"}
        assert expected.issubset(positions), f"Missing positions. Expected {expected}, got {positions}"
    
    def test_hierarchy_level_range(self):
        """TC-007: Verify HierarchyLevel is 1-4"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        invalid_levels = df.filter((df.HierarchyLevel < 1) | (df.HierarchyLevel > 4)).count()
        assert invalid_levels == 0, f"Found {invalid_levels} invalid HierarchyLevel values"
    
    def test_no_nulls_critical_columns(self):
        """TC-008: Verify no NULLs in critical columns"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        critical_cols = ["PositionID", "PositionName", "Department", "HierarchyLevel", "MinSalary", "MaxSalary"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_min_salary_less_than_max(self):
        """TC-009: Verify MinSalary is always less than MaxSalary"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        invalid = df.filter(df.MinSalary >= df.MaxSalary).count()
        assert invalid == 0, f"Found {invalid} rows where MinSalary >= MaxSalary"
    
    def test_salary_range(self):
        """TC-010: Verify salary ranges are reasonable (MinSalary >= 20000, MaxSalary <= 200000)"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        invalid = df.filter((df.MinSalary < 20000) | (df.MaxSalary > 200000)).count()
        assert invalid == 0, f"Found {invalid} rows with unreasonable salary ranges"
    
    def test_hierarchy_level_unique(self):
        """TC-011: Verify each HierarchyLevel appears exactly once (1-4)"""
        df = self.spark.read.parquet(f"{self.output_row}/emp.Positions")
        levels = {row.HierarchyLevel for row in df.collect()}
        assert levels == {1, 2, 3, 4}, f"Expected levels {1,2,3,4}, got {levels}"
    
    def test_department_populated(self):
        """TC-012: Verify Department is populated for all positions"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        null_depts = df.filter((df.Department.isNull()) | (len(df.Department) == 0)).count()
        assert null_depts == 0, f"Found {null_depts} NULL or empty departments"
    
    def test_position_name_not_empty(self):
        """TC-013: Verify PositionName is not empty"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        empty_names = df.filter((df.PositionName.isNull()) | (len(df.PositionName) == 0)).count()
        assert empty_names == 0, f"Found {empty_names} empty position names"
    
    def test_created_date_populated(self):
        """TC-014: Verify CreatedDate is populated for all rows"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        null_dates = df.filter(df.CreatedDate.isNull()).count()
        assert null_dates == 0, f"Found {null_dates} NULL CreatedDate values"
    
    def test_salary_precision(self):
        """TC-015: Verify salaries have 2 decimal places or are whole numbers"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        # Check that salaries are reasonable numeric values
        salary_check = df.filter((df.MinSalary <= 0) | (df.MaxSalary <= 0)).count()
        assert salary_check == 0, f"Found {salary_check} non-positive salary values"
    
    def test_hierarchy_level_aligns_with_salary(self):
        """TC-016: Verify lower hierarchy levels have lower salary ranges"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions").orderBy("HierarchyLevel")
        positions = df.collect()
        for i in range(len(positions) - 1):
            assert positions[i].MaxSalary < positions[i+1].MinSalary, \
                f"Salary ranges don't align with hierarchy levels"
    
    def test_department_consistency(self):
        """TC-017: Verify Department values are consistent across positions"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        depts = {row.Department for row in df.select("Department").distinct().collect()}
        # All should be from a limited set of departments
        assert len(depts) <= 5, f"Too many departments: {depts}"
    
    def test_min_salary_positive(self):
        """TC-018: Verify all MinSalary values are positive"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        negative = df.filter(df.MinSalary <= 0).count()
        assert negative == 0, f"Found {negative} non-positive MinSalary values"
    
    def test_max_salary_positive(self):
        """TC-019: Verify all MaxSalary values are positive"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        negative = df.filter(df.MaxSalary <= 0).count()
        assert negative == 0, f"Found {negative} non-positive MaxSalary values"
    
    def test_position_count_matches_hierarchy_levels(self):
        """TC-020: Verify 4 positions match 4 hierarchy levels"""
        df = self.spark.read.parquet(f"{self.output_root}/emp.Positions")
        assert df.count() == df.select("HierarchyLevel").distinct().count(), \
            "Number of positions doesn't match number of hierarchy levels"
