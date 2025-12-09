"""
Comprehensive test suite for 13_dbo_audit_system_error.py module.
"""

import pytest
from pyspark.sql import SparkSession
import tempfile
import shutil
import os
import sys

sys.path.insert(0, '/mnt/user-data/uploads')
from utils import get_spark
import importlib.util

def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

dbo_mod = load_module("dbo", "/mnt/user-data/uploads/13_dbo_audit_system_error.py")


class TestDboModule:
    """Test suite for dbo module."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_system_configuration_created(self, temp_dir):
        """Test that system configuration is created."""
        dbo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        config = spark.read.parquet(f"{temp_dir}/dbo.SystemConfiguration")
        assert config.count() > 0
        spark.stop()
    
    def test_audit_log_created(self, temp_dir):
        """Test that audit log structure is created."""
        dbo_mod.main(temp_dir)
        
        output_path = f"{temp_dir}/dbo.AuditLog"
        assert os.path.exists(output_path)
    
    def test_error_log_created(self, temp_dir):
        """Test that error log structure is created."""
        dbo_mod.main(temp_dir)
        
        output_path = f"{temp_dir}/dbo.ErrorLog"
        assert os.path.exists(output_path)
    
    def test_system_config_has_data(self, temp_dir):
        """Test that system configuration has data."""
        dbo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        config = spark.read.parquet(f"{temp_dir}/dbo.SystemConfiguration")
        
        # Check for default tax rate config
        tax_config = config.filter("ConfigKey = 'default.tax.rate'").count()
        assert tax_config == 1
        spark.stop()
    
    def test_audit_log_has_correct_schema(self, temp_dir):
        """Test that audit log has correct schema."""
        dbo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        audit_log = spark.read.parquet(f"{temp_dir}/dbo.AuditLog")
        
        expected_columns = [
            "AuditID", "TableName", "RecordID", "Action", "OldValues",
            "NewValues", "ChangedBy", "ChangedDate", "IPAddress", "ApplicationName"
        ]
        
        assert set(expected_columns) == set(audit_log.columns)
        spark.stop()
    
    def test_error_log_has_correct_schema(self, temp_dir):
        """Test that error log has correct schema."""
        dbo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        error_log = spark.read.parquet(f"{temp_dir}/dbo.ErrorLog")
        
        expected_columns = [
            "ErrorID", "ErrorNumber", "ErrorSeverity", "ErrorState",
            "ErrorProcedure", "ErrorLine", "ErrorMessage", "UserName",
            "HostName", "ApplicationName", "ErrorDate"
        ]
        
        assert set(expected_columns) == set(error_log.columns)
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
