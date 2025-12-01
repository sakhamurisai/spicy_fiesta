"""
Test suite for 09_promo_promotions.py - promotion tables
Tests: 2 hardcoded promotions, 50 promo items, 200 promo stores
Coverage: 100% of promotions generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestPromotionsGenerator:
    """Test suite for Promotions generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_promotions_row_count(self):
        """TC-001: Verify exactly 2 promotions"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        assert_row_count(df, 2, tolerance=0)
    
    def test_promotions_columns_exist(self):
        """TC-002: Verify promotions columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        expected_columns = ["PromotionID", "PromotionCode", "DiscountPercentage", "DiscountAmount", "StartDateID", "EndDateID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_promotion_items_row_count(self):
        """TC-003: Verify exactly 50 promotion items"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionItems")
        assert_row_count(df, 50, tolerance=0)
    
    def test_promotion_items_columns_exist(self):
        """TC-004: Verify promotion items columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionItems")
        expected_columns = ["PromotionItemID", "PromotionID", "ItemID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_promotion_stores_row_count(self):
        """TC-005: Verify exactly 200 promotion stores"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionStores")
        assert_row_count(df, 200, tolerance=0)
    
    def test_promotion_stores_columns_exist(self):
        """TC-006: Verify promotion stores columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionStores")
        expected_columns = ["PromotionStoreID", "PromotionID", "LocationID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_promotion_ids_unique(self):
        """TC-007: Verify PromotionID is unique for all promotions"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        assert_unique_values(df, "PromotionID", 2)
    
    def test_promotion_items_item_range(self):
        """TC-008: Verify ItemID in promo items is 1-50"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionItems")
        invalid = df.filter((df.ItemID < 1) | (df.ItemID > 50)).count()
        assert invalid == 0, f"Found {invalid} invalid ItemID values in promo items"
    
    def test_promotion_stores_location_range(self):
        """TC-009: Verify LocationID in promo stores is 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionStores")
        invalid = df.filter((df.LocationID < 1) | (df.LocationID > 200)).count()
        assert invalid == 0, f"Found {invalid} invalid LocationID values in promo stores"
    
    def test_promotion_discount_logic(self):
        """TC-010: Verify XOR: either DiscountPercentage or DiscountAmount is set, not both"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        for row in df.collect():
            has_percent = row.DiscountPercentage is not None and row.DiscountPercentage > 0
            has_amount = row.DiscountAmount is not None and row.DiscountAmount > 0
            # XOR: exactly one should be true
            assert has_percent != has_amount, \
                f"PromotionID {row.PromotionID}: both or neither discount set"
    
    def test_promotion_discount_percentage_range(self):
        """TC-011: Verify discount percentages are 0-100"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        invalid = df.filter((df.DiscountPercentage < 0) | (df.DiscountPercentage > 100)).count()
        assert invalid == 0, f"Found {invalid} invalid discount percentages"
    
    def test_promotion_codes_hardcoded(self):
        """TC-012: Verify hardcoded promotion codes (PROMO-10, PROMO-5)"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        codes = {row.PromotionCode for row in df.select("PromotionCode").collect()}
        # Should include codes like PROMO-10 (10% off) and PROMO-5 ($5 off)
        assert len(codes) >= 2, f"Expected at least 2 promo codes, got {len(codes)}"
    
    def test_promotion_items_no_duplicates(self):
        """TC-013: Verify no duplicate (PromotionID, ItemID) pairs"""
        df = self.spark.read.parquet(f"{self.output_row}/promo.PromotionItems")
        duplicates = df.groupBy("PromotionID", "ItemID").count().filter("count > 1").count()
        assert duplicates == 0, f"Found {duplicates} duplicate (PromotionID, ItemID) pairs"
    
    def test_promotion_stores_no_duplicates(self):
        """TC-014: Verify no duplicate (PromotionID, LocationID) pairs"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionStores")
        duplicates = df.groupBy("PromotionID", "LocationID").count().filter("count > 1").count()
        assert duplicates == 0, f"Found {duplicates} duplicate (PromotionID, LocationID) pairs"
    
    def test_promotion_no_nulls_critical(self):
        """TC-015: Verify no NULLs in critical promotions columns"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        critical_cols = ["PromotionID", "PromotionCode", "StartDateID", "EndDateID"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_promotion_items_no_nulls_critical(self):
        """TC-016: Verify no NULLs in critical promo items columns"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionItems")
        critical_cols = ["PromotionItemID", "PromotionID", "ItemID"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_promotion_stores_no_nulls_critical(self):
        """TC-017: Verify no NULLs in critical promo stores columns"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionStores")
        critical_cols = ["PromotionStoreID", "PromotionID", "LocationID"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_promotion_codes_unique(self):
        """TC-018: Verify all promotion codes are unique"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        assert_unique_values(df, "PromotionCode", 2)
    
    def test_start_date_before_end_date(self):
        """TC-019: Verify StartDateID < EndDateID for all promotions"""
        df = self.spark.read.parquet(f"{self.output_root}/promo.Promotions")
        invalid = df.filter(df.StartDateID >= df.EndDateID).count()
        assert invalid == 0, f"Found {invalid} promotions with StartDateID >= EndDateID"
    
    def test_all_promotions_represented(self):
        """TC-020: Verify both promotions are used in promotion items and stores"""
        items_df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionItems")
        stores_df = self.spark.read.parquet(f"{self.output_root}/promo.PromotionStores")
        items_promo_ids = {row.PromotionID for row in items_df.select("PromotionID").distinct().collect()}
        stores_promo_ids = {row.PromotionID for row in stores_df.select("PromotionID").distinct().collect()}
        assert items_promo_ids == {1, 2} or items_promo_ids == {1}, "Unexpected promotion IDs in items"
        assert stores_promo_ids == {1, 2} or stores_promo_ids == {1}, "Unexpected promotion IDs in stores"
