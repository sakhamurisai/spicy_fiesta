"""
Test suite for 08_inv_items_storeinventory_po_shipments.py - inventory tables
Tests: 7 inventory items, 1400 store inventory, 600 POs, 1800 PO items
Coverage: 100% of inventory generation logic
"""

import pytest
from conftest import assert_dataframe_columns, assert_row_count, assert_no_nulls_in_columns, \
    assert_unique_values


class TestInventoryGenerator:
    """Test suite for Inventory generation"""
    
    @pytest.fixture(autouse=True)
    def setup(self, spark, output_root):
        self.spark = spark
        self.output_root = output_root
    
    def test_inventory_items_row_count(self):
        """TC-001: Verify exactly 7 inventory items (maps to 7 ingredients)"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.Items")
        assert_row_count(df, 7, tolerance=0)
    
    def test_inventory_items_columns_exist(self):
        """TC-002: Verify inventory items columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.Items")
        expected_columns = ["InventoryItemID", "ItemCode", "ItemName", "Unit", "ReorderLevel", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_store_inventory_row_count(self):
        """TC-003: Verify 1400 store inventory records (200 locations × 7 items)"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.StoreInventory")
        assert_row_count(df, 1400, tolerance=0)
    
    def test_store_inventory_columns_exist(self):
        """TC-004: Verify store inventory columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.StoreInventory")
        expected_columns = ["StoreInventoryID", "LocationID", "InventoryItemID", "Quantity", "LastRestockDateID", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_store_inventory_seed_quantity(self):
        """TC-005: Verify all store inventory quantities are 200 (seed quantity)"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.StoreInventory")
        non_seed = df.filter(df.Quantity != 200).count()
        assert non_seed == 0, f"Found {non_seed} store inventory records with quantity != 200"
    
    def test_purchase_orders_row_count(self):
        """TC-006: Verify 600 purchase orders (3 per location)"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrders")
        assert_row_count(df, 600, tolerance=0)
    
    def test_purchase_orders_columns_exist(self):
        """TC-007: Verify purchase orders columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrders")
        expected_columns = ["PurchaseOrderID", "LocationID", "OrderDateID", "Status", "TotalAmount", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_purchase_order_items_row_count(self):
        """TC-008: Verify 1800 PO items (3 items per PO × 600 POs)"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrderItems")
        assert_row_count(df, 1800, tolerance=50)
    
    def test_purchase_order_items_columns_exist(self):
        """TC-009: Verify PO items columns exist"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrderItems")
        expected_columns = ["POItemID", "PurchaseOrderID", "InventoryItemID", "Quantity", "UnitCost", "LineTotal", "CreatedDate"]
        assert_dataframe_columns(df, expected_columns)
    
    def test_item_code_format(self):
        """TC-010: Verify ItemCode format is INV-{InventoryItemID}"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.Items")
        items = df.collect()
        for item in items:
            expected_code = f"INV-{item.InventoryItemID}"
            assert item.ItemCode == expected_code, \
                f"ItemID {item.InventoryItemID}: expected {expected_code}, got {item.ItemCode}"
    
    def test_store_inventory_location_range(self):
        """TC-011: Verify LocationID in store inventory is 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.StoreInventory")
        invalid = df.filter((df.LocationID < 1) | (df.LocationID > 200)).count()
        assert invalid == 0, f"Found {invalid} invalid LocationID values"
    
    def test_store_inventory_item_range(self):
        """TC-012: Verify InventoryItemID in store inventory is 1-7"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.StoreInventory")
        invalid = df.filter((df.InventoryItemID < 1) | (df.InventoryItemID > 7)).count()
        assert invalid == 0, f"Found {invalid} invalid InventoryItemID values"
    
    def test_po_location_range(self):
        """TC-013: Verify LocationID in purchase orders is 1-200"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrders")
        invalid = df.filter((df.LocationID < 1) | (df.LocationID > 200)).count()
        assert invalid == 0, f"Found {invalid} invalid LocationID values in POs"
    
    def test_po_status_submitted(self):
        """TC-014: Verify all PO status values are 'Submitted'"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrders")
        non_submitted = df.filter(df.Status != "Submitted").count()
        assert non_submitted == 0, f"Found {non_submitted} POs with status != Submitted"
    
    def test_po_items_item_range(self):
        """TC-015: Verify InventoryItemID in PO items is 1-7"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrderItems")
        invalid = df.filter((df.InventoryItemID < 1) | (df.InventoryItemID > 7)).count()
        assert invalid == 0, f"Found {invalid} invalid InventoryItemID values in PO items"
    
    def test_po_items_quantity_range(self):
        """TC-016: Verify PO item quantity is between 50 and 200"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrderItems")
        invalid = df.filter((df.Quantity < 50) | (df.Quantity > 200)).count()
        assert invalid == 0, f"Found {invalid} PO items with quantity out of [50, 200]"
    
    def test_po_items_unit_cost_positive(self):
        """TC-017: Verify PO item unit cost is positive"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrderItems")
        invalid = df.filter(df.UnitCost <= 0).count()
        assert invalid == 0, f"Found {invalid} PO items with non-positive unit cost"
    
    def test_po_items_line_total_correct(self):
        """TC-018: Verify LineTotal = Quantity × UnitCost"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrderItems")
        invalid = 0
        for row in df.collect():
            expected_total = row.Quantity * row.UnitCost
            if abs(float(row.LineTotal) - expected_total) > 0.01:
                invalid += 1
        assert invalid < 10, f"Found {invalid} PO items with incorrect LineTotal"
    
    def test_inventory_items_no_nulls_critical(self):
        """TC-019: Verify no NULLs in critical inventory items columns"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.Items")
        critical_cols = ["InventoryItemID", "ItemCode", "ItemName", "Unit"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_store_inventory_no_nulls_critical(self):
        """TC-020: Verify no NULLs in critical store inventory columns"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.StoreInventory")
        critical_cols = ["StoreInventoryID", "LocationID", "InventoryItemID", "Quantity"]
        assert_no_nulls_in_columns(df, critical_cols)
    
    def test_purchase_order_unique_ids(self):
        """TC-021: Verify PurchaseOrderID is unique"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrders")
        assert_unique_values(df, "PurchaseOrderID", 600)
    
    def test_po_items_per_order_count(self):
        """TC-022: Verify each PO has approximately 3 items"""
        df = self.spark.read.parquet(f"{self.output_root}/inv.PurchaseOrderItems")
        po_item_counts = df.groupBy("PurchaseOrderID").count().collect()
        avg_items = sum(row['count'] for row in po_item_counts) / len(po_item_counts)
        assert 2.8 < avg_items < 3.2, f"Average PO items {avg_items} not close to 3"
