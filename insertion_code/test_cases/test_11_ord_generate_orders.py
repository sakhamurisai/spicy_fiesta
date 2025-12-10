"""
Comprehensive test suite for 11_ord_generate_orders.py module.
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


class TestOrdersModule:
    """Test suite for orders generation module."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        
        # Create all prerequisites
        calendar_mod.main(temp_path)
        states_mod.main(temp_path)
        locations_mod.main(temp_path, 5)
        menu_mod.main(temp_path)
        inventory_mod.main(temp_path)
        loyalty_mod.main(temp_path, 100)
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_orders_created(self, temp_dir):
        """Test that orders are created."""
        orders_mod.main(temp_dir, 100)
        
        spark = get_spark("test_read")
        orders = spark.read.parquet(f"{temp_dir}/ord.Orders")
        assert orders.count() > 0
        spark.stop()
    
    def test_order_items_created(self, temp_dir):
        """Test that order items are created."""
        orders_mod.main(temp_dir, 100)
        
        spark = get_spark("test_read")
        order_items = spark.read.parquet(f"{temp_dir}/ord.OrderItems")
        assert order_items.count() > 0
        spark.stop()
    
    def test_payments_created(self, temp_dir):
        """Test that payment transactions are created."""
        orders_mod.main(temp_dir, 100)
        
        spark = get_spark("test_read")
        payments = spark.read.parquet(f"{temp_dir}/ord.PaymentTransactions")
        assert payments.count() > 0
        spark.stop()
    
    def test_receipts_created(self, temp_dir):
        """Test that receipts are created."""
        orders_mod.main(temp_dir, 100)
        
        spark = get_spark("test_read")
        receipts = spark.read.parquet(f"{temp_dir}/ord.Receipts")
        assert receipts.count() > 0
        spark.stop()
    
    def test_order_amounts_positive(self, temp_dir):
        """Test that order amounts are positive."""
        orders_mod.main(temp_dir, 100)
        
        spark = get_spark("test_read")
        orders = spark.read.parquet(f"{temp_dir}/ord.Orders")
        
        assert orders.filter("TotalAmount <= 0").count() == 0
        assert orders.filter("SubtotalAmount <= 0").count() == 0
        spark.stop()
    
    def test_order_channels_created(self, temp_dir):
        """Test that order channels are created."""
        orders_mod.main(temp_dir, 100)
        
        spark = get_spark("test_read")
        channels = spark.read.parquet(f"{temp_dir}/ord.OrderChannels")
        assert channels.count() > 0
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
