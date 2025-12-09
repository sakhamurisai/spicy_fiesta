"""
Comprehensive test suite for 08_inv_items_storeinventory_po_shipments.py module.
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

states_mod = load_module("states", "/mnt/user-data/uploads/01_store_states.py")
locations_mod = load_module("locations", "/mnt/user-data/uploads/02_store_locations.py")
menu_mod = load_module("menu", "/mnt/user-data/uploads/07_menu_categories_items_recipes.py")
inventory_mod = load_module("inventory", "/mnt/user-data/uploads/08_inv_items_storeinventory_po_shipments.py")


class TestInventoryModule:
    """Test suite for inventory module."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        
        # Create prerequisites
        states_mod.main(temp_path)
        locations_mod.main(temp_path, 5)
        menu_mod.main(temp_path)
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_inventory_items_created(self, temp_dir):
        """Test that inventory items are created."""
        inventory_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        inv_items = spark.read.parquet(f"{temp_dir}/inv.Items")
        assert inv_items.count() > 0
        spark.stop()
    
    def test_store_inventory_created(self, temp_dir):
        """Test that store inventory is created."""
        inventory_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        store_inv = spark.read.parquet(f"{temp_dir}/inv.StoreInventory")
        assert store_inv.count() > 0
        spark.stop()
    
    def test_purchase_orders_created(self, temp_dir):
        """Test that purchase orders are created."""
        inventory_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        po = spark.read.parquet(f"{temp_dir}/inv.PurchaseOrders")
        assert po.count() > 0
        spark.stop()
    
    def test_purchase_order_items_created(self, temp_dir):
        """Test that PO items are created."""
        inventory_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        poi = spark.read.parquet(f"{temp_dir}/inv.PurchaseOrderItems")
        assert poi.count() > 0
        spark.stop()
    
    def test_inventory_quantities_positive(self, temp_dir):
        """Test that inventory quantities are positive."""
        inventory_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        store_inv = spark.read.parquet(f"{temp_dir}/inv.StoreInventory")
        
        negative = store_inv.filter("QuantityOnHand < 0").count()
        assert negative == 0
        spark.stop()
    
    def test_po_amounts_positive(self, temp_dir):
        """Test that PO amounts are positive."""
        inventory_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        po = spark.read.parquet(f"{temp_dir}/inv.PurchaseOrders")
        
        negative = po.filter("TotalAmount <= 0").count()
        assert negative == 0
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
