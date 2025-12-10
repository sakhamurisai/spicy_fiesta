"""
Comprehensive test suite for 12_finance_daily_sales_store_expenses.py module.
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
menu_mod = load_module("menu", os.path.join(TEST_DIR, "07_menu_categories_items_recipes.py"))
inventory_mod = load_module("inventory", os.path.join(TEST_DIR, "08_inv_items_storeinventory_po_shipments.py"))
loyalty_mod = load_module("loyalty", os.path.join(TEST_DIR, "10_loyalty_members_points_rewards.py"))
orders_mod = load_module("orders", os.path.join(TEST_DIR, "11_ord_generate_orders.py"))
finance_mod = load_module("finance", os.path.join(TEST_DIR, "12_finance_daily_sales_store_expenses.py"))


class TestFinanceModule:
    """Test suite for finance module."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        
        # Create all prerequisites
        calendar_mod.main(temp_path)
        states_mod.main(temp_path)
        locations_mod.main(temp_path, 5)
        menu_mod.main(temp_path)
        inventory_mod.main(temp_path)
        loyalty_mod.main(temp_path, 50)
        orders_mod.main(temp_path, 100)
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_daily_sales_summary_created(self, temp_dir):
        """Test that daily sales summary is created."""
        finance_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        sales = spark.read.parquet(f"{temp_dir}/finance.DailySalesSummary")
        assert sales.count() > 0
        spark.stop()
    
    def test_store_expenses_created(self, temp_dir):
        """Test that store expenses are created."""
        finance_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        expenses = spark.read.parquet(f"{temp_dir}/finance.StoreExpenses")
        assert expenses.count() > 0
        spark.stop()
    
    def test_sales_amounts_positive(self, temp_dir):
        """Test that sales amounts are positive."""
        finance_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        sales = spark.read.parquet(f"{temp_dir}/finance.DailySalesSummary")
        
        assert sales.filter("GrossSales < 0").count() == 0
        assert sales.filter("NetSales < 0").count() == 0
        spark.stop()
    
    def test_expense_amounts_positive(self, temp_dir):
        """Test that expense amounts are positive."""
        finance_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        expenses = spark.read.parquet(f"{temp_dir}/finance.StoreExpenses")
        
        assert expenses.filter("Amount <= 0").count() == 0
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
