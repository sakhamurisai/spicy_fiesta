"""
Comprehensive test suite for 05_emp_employees.py module.
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

states_mod = load_module("states", os.path.join(TEST_DIR, "01_store_states.py"))
locations_mod = load_module("locations", os.path.join(TEST_DIR, "02_store_locations.py"))
positions_mod = load_module("positions", os.path.join(TEST_DIR, "04_emp_positions.py"))
employees_mod = load_module("employees", os.path.join(TEST_DIR, "05_emp_employees.py"))


class TestEmployeesDataFrame:
    """Test suite for create_employees_dataframe function."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = get_spark("test_employees")
        yield spark
        spark.stop()
    
    @pytest.fixture(scope="class")
    def setup_data(self, spark):
        temp_dir = tempfile.mkdtemp()
        states_mod.main(temp_dir)
        locations_mod.main(temp_dir, 5)
        positions_mod.main(temp_dir)
        
        positions_df = spark.read.parquet(f"{temp_dir}/emp.Positions").select("PositionID")
        locations_df = spark.read.parquet(f"{temp_dir}/store.Locations").select("LocationID")
        
        yield temp_dir, positions_df, locations_df
        shutil.rmtree(temp_dir, ignore_errors=True)
    
    def test_create_employees_success(self, spark, setup_data):
        """Test successful employees creation."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 10, positions_df, locations_df)
        assert emp_df is not None
        assert emp_df.count() == 10
    
    def test_employees_has_required_columns(self, spark, setup_data):
        """Test all required columns exist."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 5, positions_df, locations_df)
        
        expected_columns = [
            "EmployeeID", "EmployeeGUID", "EmployeeNumber", "FirstName", "LastName",
            "Email", "DateOfBirth", "HireDateID", "PositionID", "PrimaryLocationID",
            "HourlyRate", "PasswordHash", "PasswordSalt", "IsActive", "CreatedDate"
        ]
        
        assert set(expected_columns) == set(emp_df.columns)
    
    def test_employee_ids_unique(self, spark, setup_data):
        """Test that EmployeeIDs are unique."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 20, positions_df, locations_df)
        
        total = emp_df.count()
        distinct = emp_df.select("EmployeeID").distinct().count()
        assert total == distinct
    
    def test_employee_numbers_unique(self, spark, setup_data):
        """Test that EmployeeNumbers are unique."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 15, positions_df, locations_df)
        
        total = emp_df.count()
        distinct = emp_df.select("EmployeeNumber").distinct().count()
        assert total == distinct
    
    def test_employee_guids_unique(self, spark, setup_data):
        """Test that EmployeeGUIDs are unique."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 10, positions_df, locations_df)
        
        total = emp_df.count()
        distinct = emp_df.select("EmployeeGUID").distinct().count()
        assert total == distinct
    
    def test_email_format(self, spark, setup_data):
        """Test that emails are formatted correctly."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 5, positions_df, locations_df)
        
        emails = [row.Email for row in emp_df.select("Email").collect()]
        for email in emails:
            assert "@spicyfiesta.com" in email
            assert "." in email
    
    def test_all_employees_active(self, spark, setup_data):
        """Test that all employees are active."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 10, positions_df, locations_df)
        
        inactive = emp_df.filter("IsActive != 1").count()
        assert inactive == 0
    
    def test_hourly_rate_positive(self, spark, setup_data):
        """Test that hourly rates are positive."""
        _, positions_df, locations_df = setup_data
        
        emp_df = employees_mod.create_employees_dataframe(spark, 10, positions_df, locations_df)
        
        negative = emp_df.filter("HourlyRate <= 0").count()
        assert negative == 0
    
    def test_invalid_num_employees_raises_error(self, spark, setup_data):
        """Test that invalid employee count raises error."""
        _, positions_df, locations_df = setup_data
        
        with pytest.raises(ValueError):
            employees_mod.create_employees_dataframe(spark, 0, positions_df, locations_df)
    
    def test_none_spark_raises_error(self, setup_data):
        """Test that None spark raises ValueError."""
        _, positions_df, locations_df = setup_data
        
        with pytest.raises(ValueError):
            employees_mod.create_employees_dataframe(None, 10, positions_df, locations_df)


class TestEmployeesMain:
    """Test suite for main function."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        states_mod.main(temp_path)
        locations_mod.main(temp_path, 3)
        positions_mod.main(temp_path)
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_main_creates_parquet(self, temp_dir):
        """Test that main creates parquet file."""
        employees_mod.main(temp_dir, 10)
        
        output_path = os.path.join(temp_dir, "emp.Employees")
        assert os.path.exists(output_path)
    
    def test_main_correct_count(self, temp_dir):
        """Test correct number of employees created."""
        num_emp = 25
        employees_mod.main(temp_dir, num_emp)
        
        spark = get_spark("test_read")
        emp_df = spark.read.parquet(f"{temp_dir}/emp.Employees")
        assert emp_df.count() == num_emp
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
