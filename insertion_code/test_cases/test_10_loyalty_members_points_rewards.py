"""
Comprehensive test suite for 10_loyalty_members_points_rewards.py module.
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

loyalty_mod = load_module("loyalty", os.path.join(TEST_DIR, "10_loyalty_members_points_rewards.py"))


class TestLoyaltyModule:
    """Test suite for loyalty module."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_members_created(self, temp_dir):
        """Test that members are created."""
        loyalty_mod.main(temp_dir, 100)
        
        spark = get_spark("test_read")
        members = spark.read.parquet(f"{temp_dir}/loyalty.Members")
        assert members.count() == 100
        spark.stop()
    
    def test_rewards_created(self, temp_dir):
        """Test that rewards are created."""
        loyalty_mod.main(temp_dir, 50)
        
        spark = get_spark("test_read")
        rewards = spark.read.parquet(f"{temp_dir}/loyalty.Rewards")
        assert rewards.count() > 0
        spark.stop()
    
    def test_member_numbers_unique(self, temp_dir):
        """Test that member numbers are unique."""
        loyalty_mod.main(temp_dir, 50)
        
        spark = get_spark("test_read")
        members = spark.read.parquet(f"{temp_dir}/loyalty.Members")
        
        total = members.count()
        distinct = members.select("MemberNumber").distinct().count()
        assert total == distinct
        spark.stop()
    
    def test_member_emails_unique(self, temp_dir):
        """Test that member emails are unique."""
        loyalty_mod.main(temp_dir, 50)
        
        spark = get_spark("test_read")
        members = spark.read.parquet(f"{temp_dir}/loyalty.Members")
        
        total = members.count()
        distinct = members.select("Email").distinct().count()
        assert total == distinct
        spark.stop()
    
    def test_reward_points_positive(self, temp_dir):
        """Test that reward points costs are positive."""
        loyalty_mod.main(temp_dir, 50)
        
        spark = get_spark("test_read")
        rewards = spark.read.parquet(f"{temp_dir}/loyalty.Rewards")
        
        negative = rewards.filter("PointsCost <= 0").count()
        assert negative == 0
        spark.stop()
    
    def test_rewards_active(self, temp_dir):
        """Test that rewards are active."""
        loyalty_mod.main(temp_dir, 50)
        
        spark = get_spark("test_read")
        rewards = spark.read.parquet(f"{temp_dir}/loyalty.Rewards")
        
        inactive = rewards.filter("IsActive != 1").count()
        assert inactive == 0
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
