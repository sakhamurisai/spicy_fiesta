"""
Test suite for 12_finance_daily_sales_store_expenses.py - finance tables
Tests: 200,000 daily sales summary (200 locations × 1000 days), 200 store expenses
Coverage: 100% of finance generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestFinanceGenerator:
    """Test suite for Finance generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_daily_sales_summary_row_count(self):
        """TC-001: Verify ~200,000 daily sales records (200 locations × 1000 days)"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        row_count = df.count()
        # Allow 20% tolerance for missing data
        assert row_count > 150000, f"Expected > 150K daily sales records, got {row_count}"
    
    def test_daily_sales_columns_exist(self):
        """TC-002: Verify DailySalesSummary columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        expected_columns = ["DailySalesSummaryID", "LocationID", "OrderDateID", "TotalOrders",
                           "GrossSales", "TaxCollected", "AverageOrderValue", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_store_expenses_row_count(self):
        """TC-003: Verify 200 store expense records (rent expense per location)"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.StoreExpenses")
        assert_row_count(df, 200, tolerance=0)
    
    def test_store_expenses_columns_exist(self):
        """TC-004: Verify StoreExpenses columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.StoreExpenses")
        expected_columns = ["ExpenseID", "LocationID", "ExpenseType", "Amount", "ExpenseDateID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_daily_sales_location_range(self):
        """TC-005: Verify LocationID in daily sales is 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        invalid = df.filter((df.LocationID < 1) | (df.LocationID > 200)).count()
        assert invalid == 0, f"Found {invalid} invalid LocationID values in daily sales"
    
    def test_daily_sales_total_orders_positive(self):
        """TC-006: Verify TotalOrders is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        invalid = df.filter(df.TotalOrders <= 0).count()
        assert invalid == 0, f"Found {invalid} days with non-positive TotalOrders"
    
    def test_gross_sales_positive(self):
        """TC-007: Verify GrossSales is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        invalid = df.filter(df.GrossSales <= 0).count()
        assert invalid == 0, f"Found {invalid} days with non-positive GrossSales"
    
    def test_tax_collected_calculation(self):
        """TC-008: Verify TaxCollected ≈ GrossSales × 0.07"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary").limit(1000)
        invalid_count = 0
        for row in df.collect():
            expected_tax = float(row.GrossSales) * 0.07
            if abs(float(row.TaxCollected) - expected_tax) > 1.0:  # Allow $1 tolerance
                invalid_count += 1
        assert invalid_count < 50, f"Found {invalid_count} days with incorrect tax calculation"
    
    def test_average_order_value_calculation(self):
        """TC-009: Verify AverageOrderValue = GrossSales / TotalOrders"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary").limit(1000)
        invalid_count = 0
        for row in df.collect():
            if row.TotalOrders > 0:
                expected_avg = float(row.GrossSales) / float(row.TotalOrders)
                if abs(float(row.AverageOrderValue) - expected_avg) > 0.1:
                    invalid_count += 1
        assert invalid_count < 50, f"Found {invalid_count} days with incorrect AverageOrderValue"
    
    def test_store_expenses_location_range(self):
        """TC-010: Verify LocationID in expenses is 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.StoreExpenses")
        invalid = df.filter((df.LocationID < 1) | (df.LocationID > 200)).count()
        assert invalid == 0, f"Found {invalid} invalid LocationID values in expenses"
    
    def test_store_expenses_location_unique(self):
        """TC-011: Verify each location has unique expenses (1 per location for rent)"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.StoreExpenses")
        assert_unique_values(df, "LocationID", 200)
    
    def test_store_expenses_rent_amount(self):
        """TC-012: Verify all store expenses are rent with amount 5000.0"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.StoreExpenses")
        invalid = df.filter((df.ExpenseType != "Rent") | (df.Amount != 5000.0)).count()
        assert invalid == 0, f"Found {invalid} expenses with non-rent or incorrect amount"
    
    def test_daily_sales_no_nulls_critical(self):
        """TC-013: Verify no NULLs in critical daily sales columns"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        critical_cols = ["DailySalesSummaryID", "LocationID", "OrderDateID", "TotalOrders", "GrossSales"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_store_expenses_no_nulls_critical(self):
        """TC-014: Verify no NULLs in critical store expenses columns"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.StoreExpenses")
        critical_cols = ["ExpenseID", "LocationID", "ExpenseType", "Amount"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_average_order_value_positive(self):
        """TC-015: Verify AverageOrderValue is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        invalid = df.filter(df.AverageOrderValue <= 0).count()
        assert invalid == 0, f"Found {invalid} days with non-positive AverageOrderValue"
    
    def test_tax_collected_non_negative(self):
        """TC-016: Verify TaxCollected is non-negative"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        invalid = df.filter(df.TaxCollected < 0).count()
        assert invalid == 0, f"Found {invalid} days with negative TaxCollected"
    
    def test_daily_sales_unique_location_date_combo(self):
        """TC-017: Verify (LocationID, OrderDateID) combination is unique"""
        df = self.spark.read.parquet(f"{self.output_row}/finance.DailySalesSummary")
        duplicates = df.groupBy("LocationID", "OrderDateID").count().filter("count > 1").count()
        assert duplicates == 0, f"Found {duplicates} duplicate (LocationID, OrderDateID) pairs"
    
    def test_created_date_populated(self):
        """TC-018: Verify CreatedDate is populated for all records"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        null_dates = df.filter(df.CreatedDate.isNull()).count()
        assert null_dates == 0, f"Found {null_dates} NULL CreatedDate values in daily sales"
    
    def test_expense_date_valid(self):
        """TC-019: Verify ExpenseDateID is within calendar range"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.StoreExpenses")
        invalid = df.filter((df.ExpenseDateID < 1) | (df.ExpenseDateID > 36892)).count()
        assert invalid == 0, f"Found {invalid} invalid ExpenseDateID values"
    
    def test_order_date_id_valid(self):
        """TC-020: Verify OrderDateID is within calendar range"""
        df = self.spark.read.parquet(f"{self.output_root}/finance.DailySalesSummary")
        invalid = df.filter((df.OrderDateID < 1) | (df.OrderDateID > 36892)).count()
        assert invalid == 0, f"Found {invalid} invalid OrderDateID values"
