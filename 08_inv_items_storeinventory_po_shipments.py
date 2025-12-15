"""Inventory items, store inventory, and purchase orders generation."""
from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
import random
import logging

logger = logging.getLogger(__name__)


def main():
    """Generate inventory tables."""
    spark = get_spark("inv_items")
    configure_azure_blob_storage(spark)
    
    try:
        azure_menu_path = get_azure_blob_path("menu")
        azure_store_path = get_azure_blob_path("store")
        azure_inv_path = get_azure_blob_path("inv")
        
        ing = spark.read.parquet(azure_menu_path).filter(F.col("IngredientID").isNotNull())
        
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
        write_parquet(inv_rows, azure_inv_path)
        
        locs = spark.read.parquet(azure_store_path).select("LocationID").limit(200)
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
        write_parquet(df, azure_inv_path)
        
        po_rows = []
        poi_rows = []
        next_po_id = 1
        for l in locs.collect():
            for po_num in range(1,4):
                po_id = next_po_id
                next_po_id += 1
                po_rows.append((po_id, f"PO-{po_id:06d}", int(l.LocationID), 1, None, None, "Submitted", 1000.0, 1, None, None, None))
                for iid in items[:3]:
                    qty = random.randint(50,200)
                    poi_rows.append((po_id, int(iid.InventoryItemID), qty, 0, 1.0, qty*1.0))
        
        po_df = spark.createDataFrame(po_rows, ["PurchaseOrderID","PurchaseOrderNumber","LocationID","OrderDateID","ExpectedDeliveryDateID","ActualDeliveryDateID","OrderStatus","TotalAmount","OrderedBy","ApprovedBy","ReceivedBy","CreatedDate"]) \
                     .withColumn("CreatedDate", F.current_timestamp())
        poi_df = spark.createDataFrame(poi_rows, ["PurchaseOrderID","InventoryItemID","QuantityOrdered","QuantityReceived","UnitPrice","LineTotal"]) \
                     .withColumn("PurchaseOrderItemID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp())
        write_parquet(po_df, azure_inv_path)
        write_parquet(poi_df, azure_inv_path)
        
        logger.info("Inventory tables created successfully")
        
    except Exception as e:
        logger.error(f"Error generating inventory: {str(e)}")
        raise
if __name__ == "__main__":
    main()