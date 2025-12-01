"""
Test suite for 07_menu_categories_items_recipes.py - menu tables (Categories, Items, Ingredients, RecipeItems)
Tests: 4 categories, 300 items with price formula, 7 ingredients, ~1000 recipe mappings
Coverage: 100% of menu generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestMenuGenerator:
    """Test suite for Menu generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_categories_row_count(self):
        """TC-001: Verify exactly 4 menu categories"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Categories")
        assert_row_count(df, 4, tolerance=0)
    
    def test_categories_columns_exist(self):
        """TC-002: Verify Categories columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Categories")
        expected_columns = ["CategoryID", "CategoryName", "Description", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_items_row_count(self):
        """TC-003: Verify exactly 300 menu items"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items")
        assert_row_count(df, 300, tolerance=0)
    
    def test_items_columns_exist(self):
        """TC-004: Verify Items columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items")
        expected_columns = ["ItemID", "ItemName", "CategoryID", "BasePrice", "IsActive", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_item_ids_unique(self):
        """TC-005: Verify ItemID is unique for all 300 items"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items")
        assert_unique_values(df, "ItemID", 300)
    
    def test_items_price_formula(self):
        """TC-006: Verify BasePrice formula: 1.99 + (ItemID % 10) * 0.75"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items").orderBy("ItemID")
        items = df.collect()
        for item in items:
            expected_price = 1.99 + (item.ItemID % 10) * 0.75
            # Allow small floating point tolerance
            assert abs(float(item.BasePrice) - expected_price) < 0.01, \
                f"ItemID {item.ItemID}: expected {expected_price}, got {item.BasePrice}"
    
    def test_items_category_cycling(self):
        """TC-007: Verify CategoryID cycles through 1-4 for items"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items").orderBy("ItemID")
        items = df.collect()
        for i, item in enumerate(items):
            expected_category = (i % 4) + 1
            assert item.CategoryID == expected_category, \
                f"ItemID {item.ItemID}: expected CategoryID {expected_category}, got {item.CategoryID}"
    
    def test_items_no_nulls_critical(self):
        """TC-008: Verify no NULLs in critical Items columns"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items")
        critical_cols = ["ItemID", "ItemName", "CategoryID", "BasePrice"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_ingredients_row_count(self):
        """TC-009: Verify exactly 7 ingredients"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Ingredients")
        assert_row_count(df, 7, tolerance=0)
    
    def test_ingredients_columns_exist(self):
        """TC-010: Verify Ingredients columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Ingredients")
        expected_columns = ["IngredientID", "IngredientName", "Unit", "UnitCost", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_ingredient_ids_unique(self):
        """TC-011: Verify IngredientID is unique for all 7 ingredients"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Ingredients")
        assert_unique_values(df, "IngredientID", 7)
    
    def test_ingredients_no_nulls_critical(self):
        """TC-012: Verify no NULLs in critical Ingredients columns"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Ingredients")
        critical_cols = ["IngredientID", "IngredientName", "Unit", "UnitCost"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_recipe_items_row_count(self):
        """TC-013: Verify ~1000 recipe items (2-4 per item)"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.RecipeItems")
        # 300 items × avg 3.3 ingredients = ~1000 recipes
        assert_row_count(df, 1000, tolerance=100)
    
    def test_recipe_items_columns_exist(self):
        """TC-014: Verify RecipeItems columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.RecipeItems")
        expected_columns = ["RecipeItemID", "ItemID", "IngredientID", "Quantity", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_recipe_items_no_duplicates(self):
        """TC-015: Verify no duplicate (ItemID, IngredientID) pairs in recipes"""
        df = self.spark.read.parquet(f"{self.output_row}/menu.RecipeItems")
        duplicates = df.groupBy("ItemID", "IngredientID").count().filter("count > 1").count()
        assert duplicates == 0, f"Found {duplicates} duplicate (ItemID, IngredientID) pairs"
    
    def test_recipe_items_item_range(self):
        """TC-016: Verify ItemID in recipes is 1-300"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.RecipeItems")
        invalid = df.filter((df.ItemID < 1) | (df.ItemID > 300)).count()
        assert invalid == 0, f"Found {invalid} invalid ItemID values in recipes"
    
    def test_recipe_items_ingredient_range(self):
        """TC-017: Verify IngredientID in recipes is 1-7"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.RecipeItems")
        invalid = df.filter((df.IngredientID < 1) | (df.IngredientID > 7)).count()
        assert invalid == 0, f"Found {invalid} invalid IngredientID values in recipes"
    
    def test_recipe_quantity_positive(self):
        """TC-018: Verify recipe Quantity is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.RecipeItems")
        invalid = df.filter(df.Quantity <= 0).count()
        assert invalid == 0, f"Found {invalid} non-positive Quantity values"
    
    def test_recipe_quantity_range(self):
        """TC-019: Verify recipe Quantity is between 0.05 and 2.0"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.RecipeItems")
        invalid = df.filter((df.Quantity < 0.05) | (df.Quantity > 2.0)).count()
        assert invalid == 0, f"Found {invalid} Quantity values out of range [0.05, 2.0]"
    
    def test_recipe_items_no_nulls_critical(self):
        """TC-020: Verify no NULLs in critical RecipeItems columns"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.RecipeItems")
        critical_cols = ["RecipeItemID", "ItemID", "IngredientID", "Quantity"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_categories_unique_names(self):
        """TC-021: Verify all CategoryName values are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Categories")
        assert_unique_values(df, "CategoryName", 4)
    
    def test_items_unique_names(self):
        """TC-022: Verify all ItemName values are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items")
        assert_unique_values(df, "ItemName", 300)
    
    def test_ingredients_unique_names(self):
        """TC-023: Verify all IngredientName values are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Ingredients")
        assert_unique_values(df, "IngredientName", 7)
    
    def test_base_price_positive(self):
        """TC-024: Verify all BasePrice values are positive"""
        df = self.spark.read.parquet(f"{self.output_root}/menu.Items")
        invalid = df.filter(df.BasePrice <= 0).count()
        assert invalid == 0, f"Found {invalid} non-positive BasePrice values"
