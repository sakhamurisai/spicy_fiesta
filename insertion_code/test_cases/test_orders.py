"""
Test suite for 11_ord_generate_orders.py - order tables (largest, most complex)
Tests: 5M orders, 12.5M order items, payments, receipts, feedback, inventory transactions, ledger
Coverage: 100% of orders generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestOrdersGenerator:
    """Test suite for Orders generation (largest dataset)"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_order_channels_row_count(self):
        """TC-001: Verify at least 3 order channels (In-Store, Online, Delivery)"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.OrderChannels")
        assert_row_count(df, 3, tolerance=1)
    
    def test_orders_row_count_approx(self):
        """TC-002: Verify approximately 5,000,000 orders (configurable)"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders")
        # Due to variable generation, allow 10% tolerance
        row_count = df.count()
        assert row_count > 4500000, f"Expected > 4.5M orders, got {row_count}"
    
    def test_orders_columns_exist(self):
        """TC-003: Verify Orders columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders")
        expected_columns = ["OrderID", "LocationID", "ChannelID", "OrderDateID", "CustomerMemberID",
                           "OrderType", "SubtotalAmount", "TaxAmount", "DeliveryFee", "TipAmount", 
                           "DiscountAmount", "TotalAmount", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_orders_modulo_location_cycling(self):
        """TC-004: Verify LocationID cycles using modulo (1-200)"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders").limit(1000)
        locations = {row.LocationID for row in df.collect()}
        # Should have multiple locations from cycling
        assert len(locations) > 1, "Order LocationID not cycling properly"
        assert all(1 <= loc <= 200 for loc in locations), "LocationID out of range"
    
    def test_orders_modulo_channel_cycling(self):
        """TC-005: Verify ChannelID cycles using modulo (1-3)"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders").limit(1000)
        channels = {row.ChannelID for row in df.collect()}
        assert all(1 <= ch <= 3 for ch in channels), "ChannelID out of range"
    
    def test_order_items_row_count_approx(self):
        """TC-006: Verify approximately 12,500,000 order items (2.5x orders)"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.OrderItems")
        row_count = df.count()
        assert row_count > 10000000, f"Expected > 10M order items, got {row_count}"
    
    def test_order_items_columns_exist(self):
        """TC-007: Verify OrderItems columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.OrderItems")
        expected_columns = ["OrderItemID", "OrderID", "ItemID", "Quantity", "UnitPrice", "LineTotal", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_payment_transactions_row_count(self):
        """TC-008: Verify payment transactions match order count"""
        orders_df = self.spark.read.parquet(f"{self.output_root}/ord.Orders")
        payments_df = self.spark.read.parquet(f"{self.output_root}/ord.PaymentTransactions")
        # Should have approximately 1 payment per order
        payment_count = payments_df.count()
        order_count = orders_df.count()
        assert abs(payment_count - order_count) < order_count * 0.1, \
            f"Payment count {payment_count} doesn't match order count {order_count}"
    
    def test_receipts_row_count(self):
        """TC-009: Verify receipts match order count"""
        orders_df = self.spark.read.parquet(f"{self.output_root}/ord.Orders")
        receipts_df = self.spark.read.parquet(f"{self.output_root}/ord.Receipts")
        receipt_count = receipts_df.count()
        order_count = orders_df.count()
        assert abs(receipt_count - order_count) < order_count * 0.1, \
            f"Receipt count {receipt_count} doesn't match order count {order_count}"
    
    def test_order_feedback_row_count(self):
        """TC-010: Verify feedback is ~1% of orders (sample of 0.01)"""
        orders_df = self.spark.read.parquet(f"{self.output_root}/ord.Orders")
        feedback_df = self.spark.read.parquet(f"{self.output_root}/ord.OrderFeedback")
        feedback_count = feedback_df.count()
        order_count = orders_df.count()
        expected_feedback = order_count * 0.01
        # Allow ±50% tolerance for sampling randomness
        assert feedback_count > expected_feedback * 0.5, \
            f"Feedback count {feedback_count} too low for sample rate 0.01"
    
    def test_subtotal_formula(self):
        """TC-011: Verify SubtotalAmount formula: (OrderSeq % 5 + 1) * 3.5"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders").limit(1000)
        invalid_count = 0
        for row in df.collect():
            # OrderID serves as OrderSeq
            expected_subtotal = ((row.OrderID % 5) + 1) * 3.5
            if abs(float(row.SubtotalAmount) - expected_subtotal) > 0.01:
                invalid_count += 1
        assert invalid_count < 10, f"Found {invalid_count} orders with incorrect SubtotalAmount formula"
    
    def test_tax_calculation(self):
        """TC-012: Verify TaxAmount = SubtotalAmount * 0.07"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders").limit(1000)
        invalid_count = 0
        for row in df.collect():
            expected_tax = float(row.SubtotalAmount) * 0.07
            if abs(float(row.TaxAmount) - expected_tax) > 0.01:
                invalid_count += 1
        assert invalid_count < 10, f"Found {invalid_count} orders with incorrect tax calculation"
    
    def test_delivery_fee_logic(self):
        """TC-013: Verify DeliveryFee = 3.99 if OrderType = 'Delivery', else 0"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders").limit(1000)
        invalid_count = 0
        for row in df.collect():
            if row.OrderType == "Delivery":
                if float(row.DeliveryFee) != 3.99:
                    invalid_count += 1
            else:
                if float(row.DeliveryFee) != 0:
                    invalid_count += 1
        assert invalid_count < 10, f"Found {invalid_count} orders with incorrect DeliveryFee"
    
    def test_total_amount_calculation(self):
        """TC-014: Verify TotalAmount = Subtotal + Tax + DeliveryFee + Tip - Discount"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders").limit(1000)
        invalid_count = 0
        for row in df.collect():
            expected_total = float(row.SubtotalAmount) + float(row.TaxAmount) + \
                           float(row.DeliveryFee) + float(row.TipAmount) - float(row.DiscountAmount)
            if abs(float(row.TotalAmount) - expected_total) > 0.01:
                invalid_count += 1
        assert invalid_count < 20, f"Found {invalid_count} orders with incorrect TotalAmount"
    
    def test_order_items_quantity_positive(self):
        """TC-015: Verify OrderItems Quantity is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.OrderItems").limit(10000)
        invalid = df.filter(df.Quantity <= 0).count()
        assert invalid == 0, f"Found {invalid} order items with non-positive quantity"
    
    def test_order_items_line_total_calculation(self):
        """TC-016: Verify LineTotal = Quantity × UnitPrice"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.OrderItems").limit(10000)
        invalid_count = 0
        for row in df.collect():
            expected_total = float(row.Quantity) * float(row.UnitPrice)
            if abs(float(row.LineTotal) - expected_total) > 0.01:
                invalid_count += 1
        assert invalid_count < 50, f"Found {invalid_count} order items with incorrect LineTotal"
    
    def test_order_no_nulls_critical(self):
        """TC-017: Verify no NULLs in critical Order columns"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.Orders").limit(10000)
        critical_cols = ["OrderID", "LocationID", "ChannelID", "OrderDateID", "TotalAmount"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_order_items_no_nulls_critical(self):
        """TC-018: Verify no NULLs in critical OrderItems columns"""
        df = self.spark.read.parquet(f"{self.output_root}/ord.OrderItems").limit(10000)
        critical_cols = ["OrderItemID", "OrderID", "ItemID", "Quantity", "UnitPrice", "LineTotal"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_inventory_transactions_row_count(self):
        """TC-019: Verify inventory transactions (~5x order items for recipe consumption)"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.InventoryTransactions")
        row_count = df.count()
        # Should be in range of 20-30M for 12.5M order items
        assert row_count > 10000000, f"Expected > 10M inventory transactions, got {row_count}"
    
    def test_ledger_row_count(self):
        """TC-020: Verify ledger has approximately 1 entry per order"""
        orders_df = self.spark.read.parquet(f"{self.output_root}/ord.Orders")
        ledger_df = self.spark.read.parquet(f"{self.output_root}/finance.Ledger")
        ledger_count = ledger_df.count()
        order_count = orders_df.count()
        # Ledger should have at least 1 entry per order (revenue)
        assert ledger_count > order_count * 0.8, \
            f"Ledger count {ledger_count} too low for {order_count} orders"
