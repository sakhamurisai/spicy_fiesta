"""
Comprehensive test suite for 09_promo_promotions.py module.
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
menu_mod = load_module("menu", os.path.join(TEST_DIR, "07_menu_categories_items_recipes.py"))
promo_mod = load_module("promo", os.path.join(TEST_DIR, "09_promo_promotions.py"))


class TestPromotionsModule:
    """Test suite for promotions module."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        
        # Create prerequisites
        states_mod.main(temp_path)
        locations_mod.main(temp_path, 5)
        menu_mod.main(temp_path)
        
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_promotions_created(self, temp_dir):
        """Test that promotions are created."""
        promo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        promos = spark.read.parquet(f"{temp_dir}/promo.Promotions")
        assert promos.count() > 0
        spark.stop()
    
    def test_promotion_items_created(self, temp_dir):
        """Test that promotion items are created."""
        promo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        promo_items = spark.read.parquet(f"{temp_dir}/promo.PromotionItems")
        assert promo_items.count() > 0
        spark.stop()
    
    def test_promotion_stores_created(self, temp_dir):
        """Test that promotion stores are created."""
        promo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        promo_stores = spark.read.parquet(f"{temp_dir}/promo.PromotionStores")
        assert promo_stores.count() > 0
        spark.stop()
    
    def test_promotions_active(self, temp_dir):
        """Test that promotions are active."""
        promo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        promos = spark.read.parquet(f"{temp_dir}/promo.Promotions")
        
        inactive = promos.filter("IsActive != 1").count()
        assert inactive == 0
        spark.stop()
    
    def test_promotion_values_valid(self, temp_dir):
        """Test that promotion discount values are valid."""
        promo_mod.main(temp_dir)
        
        spark = get_spark("test_read")
        promos = spark.read.parquet(f"{temp_dir}/promo.Promotions")
        
        # Check that percentage discounts are reasonable
        invalid_pct = promos.filter("DiscountPercentage IS NOT NULL AND (DiscountPercentage < 0 OR DiscountPercentage > 100)").count()
        assert invalid_pct == 0
        
        # Check that fixed amount discounts are positive
        invalid_amt = promos.filter("DiscountAmount IS NOT NULL AND DiscountAmount <= 0").count()
        assert invalid_amt == 0
        
        spark.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
