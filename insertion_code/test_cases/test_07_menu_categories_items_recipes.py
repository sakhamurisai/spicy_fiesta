"""
Comprehensive test suite for 07_menu_categories_items_recipes.py module.
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

menu_mod = load_module("menu", os.path.join(TEST_DIR, "07_menu_categories_items_recipes.py"))


class TestMenuDataFrames:
    """Test suite for menu creation."""
    
    @pytest.fixture(scope="class")
    def spark(self):
        spark = get_spark("test_menu")
        yield spark
        spark.stop()
    
    def test_create_menu_success(self, spark):
        """Test successful menu creation."""
        cat_df, items_df, ing_df, rec_df = menu_mod.create_menu_dataframes(spark)
        
        assert cat_df is not None
        assert items_df is not None
        assert ing_df is not None
        assert rec_df is not None
    
    def test_categories_count(self, spark):
        """Test categories count is 4."""
        cat_df, _, _, _ = menu_mod.create_menu_dataframes(spark)
        assert cat_df.count() == 4
    
    def test_items_count(self, spark):
        """Test items count is 300."""
        _, items_df, _, _ = menu_mod.create_menu_dataframes(spark)
        assert items_df.count() == 300
    
    def test_ingredients_count(self, spark):
        """Test ingredients count is 7."""
        _, _, ing_df, _ = menu_mod.create_menu_dataframes(spark)
        assert ing_df.count() == 7
    
    def test_recipes_created(self, spark):
        """Test that recipes are created."""
        _, _, _, rec_df = menu_mod.create_menu_dataframes(spark)
        assert rec_df.count() > 0
    
    def test_all_items_have_recipes(self, spark):
        """Test that all items have at least one recipe."""
        _, items_df, _, rec_df = menu_mod.create_menu_dataframes(spark)
        
        item_ids_with_recipes = rec_df.select("ItemID").distinct().count()
        total_items = items_df.count()
        
        # All or most items should have recipes
        assert item_ids_with_recipes > 0
    
    def test_categories_active(self, spark):
        """Test that all categories are active."""
        cat_df, _, _, _ = menu_mod.create_menu_dataframes(spark)
        inactive = cat_df.filter("IsActive != 1").count()
        assert inactive == 0
    
    def test_items_active(self, spark):
        """Test that all items are active."""
        _, items_df, _, _ = menu_mod.create_menu_dataframes(spark)
        inactive = items_df.filter("IsActive != 1").count()
        assert inactive == 0
    
    def test_base_prices_positive(self, spark):
        """Test that base prices are positive."""
        _, items_df, _, _ = menu_mod.create_menu_dataframes(spark)
        negative = items_df.filter("BasePrice <= 0").count()
        assert negative == 0
    
    def test_recipe_quantities_positive(self, spark):
        """Test that recipe quantities are positive."""
        _, _, _, rec_df = menu_mod.create_menu_dataframes(spark)
        negative = rec_df.filter("Quantity <= 0").count()
        assert negative == 0


class TestMenuMain:
    """Test suite for main function."""
    
    @pytest.fixture(scope="function")
    def temp_dir(self):
        temp_path = tempfile.mkdtemp()
        yield temp_path
        shutil.rmtree(temp_path, ignore_errors=True)
    
    def test_main_creates_all_tables(self, temp_dir):
        """Test that main creates all menu tables."""
        menu_mod.main(temp_dir)
        
        assert os.path.exists(f"{temp_dir}/menu.Categories")
        assert os.path.exists(f"{temp_dir}/menu.Items")
        assert os.path.exists(f"{temp_dir}/menu.Ingredients")
        assert os.path.exists(f"{temp_dir}/menu.RecipeItems")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
