# 08_inv_items_storeinventory_po_shipments.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("inv_items")
    # Create inv.Items aligned with menu.Ingredients
    ing = spark.read.parquet(f"{output_root}/menu.Ingredients")
    # Map Ingredients to Inventory Items (1:1)
    inv_rows = ing.withColumn("InventoryItemID", F.monotonically_increasing_id()+1) \
                 .withColumnRenamed("IngredientID","LinkedIngredientID") \
                 .withColumnRenamed("IngredientName","ItemName") \
                 .withColumn("ItemCode", F.concat(F.lit("INV-"), F.col("InventoryItemID"))) \
                 .withColumn("UnitCost", F.lit(1.0)) \
                 .withColumn("ReorderLevel", F.lit(10.0)) \
                 .withColumn("ReorderQuantity", F.lit(50.0)) \
                 .withColumn("RequiresRefrigeration", F.lit(0)) \
                 .withColumn("IsActive", F.lit(1)) \
                 .withColumn("CreatedDate", F.current_timestamp()) \
                 .select("InventoryItemID","ItemCode","ItemName","UnitOfMeasure","UnitCost","ReorderLevel","ReorderQuantity","RequiresRefrigeration","IsActive","CreatedDate")
    write_parquet(inv_rows, f"{output_root}/inv.Items")

    # StoreInventory: for each Location x InventoryItem, seed starting qty = 100..500
    locs = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID").limit(200)
    items = inv_rows.select("InventoryItemID").collect()
    rows = []
    for l in locs.collect():
        for it in items:
            rows.append((l.LocationID, int(it.InventoryItemID), float(200)))
    df = spark.createDataFrame(rows, ["LocationID","InventoryItemID","QuantityOnHand"]) \
             .withColumn("StoreInventoryID", F.monotonically_increasing_id()+1) \
             .withColumn("MinimumQuantity", F.lit(10.0)) \
             .withColumn("MaximumQuantity", F.lit(1000.0)) \
             .withColumn("LastRestockDateID", F.lit(None).cast("int")) \
             .withColumn("LastCountDateID", F.lit(None).cast("int")) \
             .withColumn("CreatedDate", F.current_timestamp()) \
             .select("StoreInventoryID","LocationID","InventoryItemID","QuantityOnHand","MinimumQuantity","MaximumQuantity","LastRestockDateID","LastCountDateID","CreatedDate")
    write_parquet(df, f"{output_root}/inv.StoreInventory")

    # PurchaseOrders & Items: Generate inbound shipments to replenish stores
    po_rows = []
    poi_rows = []
    import random
    next_po_id = 1
    for l in locs.collect():
        for po_num in range(1,4):  # 3 POs per location sample
            po_id = next_po_id; next_po_id += 1
            po_rows.append((po_id, f"PO-{po_id:06d}", int(l.LocationID), 1, None, None, "Submitted", 1000.0, 1, None, None, None))
            # items in PO
            for iid in items[:3]:  # 3 inventory items per PO
                qty = random.randint(50,200)
                poi_rows.append((po_id, int(iid.InventoryItemID), qty, 0, 1.0, qty*1.0))
    po_df = spark.createDataFrame(po_rows, ["PurchaseOrderID","PurchaseOrderNumber","LocationID","OrderDateID","ExpectedDeliveryDateID","ActualDeliveryDateID","OrderStatus","TotalAmount","OrderedBy","ApprovedBy","ReceivedBy","CreatedDate"]) \
                 .withColumn("CreatedDate", F.current_timestamp())
    poi_df = spark.createDataFrame(poi_rows, ["PurchaseOrderID","InventoryItemID","QuantityOrdered","QuantityReceived","UnitPrice","LineTotal"]) \
                 .withColumn("PurchaseOrderItemID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp())
    write_parquet(po_df, f"{output_root}/inv.PurchaseOrders")
    write_parquet(poi_df, f"{output_root}/inv.PurchaseOrderItems")

    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
