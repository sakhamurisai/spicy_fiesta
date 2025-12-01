# 11_ord_generate_orders.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F
from pyspark.sql import Window

def main(output_root="./output_parquet", total_orders=5000000):
    spark = get_spark("orders")
    total_orders = int(total_orders)

    # load dims
    cal = spark.read.parquet(f"{output_root}/dim.Calendar").select("CalendarID","CalendarDate","Year")
    items = spark.read.parquet(f"{output_root}/menu.Items").select("ItemID","ItemName","BasePrice")
    locs = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID")
    channels = spark.read.parquet(f"{output_root}/ord.OrderChannels").select("ChannelID") if spark._jsparkSession.catalog().tableExists("ord.OrderChannels") else None

    # If OrderChannels not present in output (e.g., first run), create basic channels
    try:
        channels = spark.read.parquet(f"{output_root}/ord.OrderChannels").select("ChannelID")
    except:
        ch = spark.createDataFrame([("CH-01","InStore"),("CH-02","DriveThru"),("CH-03","Mobile"),("CH-04","Web")], ["ChannelCode","ChannelName"]) \
                  .withColumn("ChannelID", F.monotonically_increasing_id()+1).select("ChannelID","ChannelCode","ChannelName")
        write_parquet(ch.select("ChannelID","ChannelCode","ChannelName"), f"{output_root}/ord.OrderChannels")
        channels = ch.select("ChannelID")

    n_locations = locs.count()
    n_items = items.count()
    max_cal_id = cal.agg({"CalendarID":"max"}).collect()[0][0]
    import math

    # Use spark.range to generate a large, efficient DF
    orders_base = spark.range(1, total_orders+1).toDF("OrderSeq")
    orders = orders_base.withColumn("OrderID", F.col("OrderSeq").cast("long")) \
        .withColumn("OrderGUID", F.expr("uuid()")) \
        .withColumn("LocationID", ((F.col("OrderSeq") % n_locations) + 1).cast("int")) \
        .withColumn("ChannelID", ((F.col("OrderSeq") % 4) + 1).cast("int")) \
        .withColumn("MemberID", ((F.col("OrderSeq") % 2000) + 1).cast("int")) \
        .withColumn("OrderDateID", ((F.col("OrderSeq") % max_cal_id) + 1).cast("int")) \
        .withColumn("OrderDateTime", F.current_timestamp()) \
        .withColumn("OrderStatus", F.lit("Completed")) \
        .withColumn("OrderType", F.expr("CASE WHEN (OrderSeq % 4)=0 THEN 'Delivery' WHEN (OrderSeq % 4)=1 THEN 'DineIn' WHEN (OrderSeq % 4)=2 THEN 'Takeout' ELSE 'DriveThru' END")) \
        .withColumn("SubtotalAmount", F.round((((F.col("OrderSeq") % 5) + 1) * 3.5),2)) \
        .withColumn("TaxAmount", F.round(F.col("SubtotalAmount") * 0.07,2)) \
        .withColumn("DiscountAmount", F.lit(0.0)) \
        .withColumn("DeliveryFee", F.expr("CASE WHEN OrderType='Delivery' THEN 3.99 ELSE 0 END")) \
        .withColumn("TipAmount", F.round((F.col("OrderSeq") % 10) * 0.5,2)) \
        .withColumn("TotalAmount", F.round(F.col("SubtotalAmount") + F.col("TaxAmount") + F.col("DeliveryFee") + F.col("TipAmount") - F.col("DiscountAmount"),2)) \
        .withColumn("PaymentMethod", F.expr("CASE WHEN (OrderSeq % 3)=0 THEN 'Cash' WHEN (OrderSeq % 3)=1 THEN 'CreditCard' ELSE 'MobileWallet' END")) \
        .withColumn("PaymentStatus", F.lit("Paid")) \
        .withColumn("PaymentDateID", F.col("OrderDateID")) \
        .withColumn("PreparedBy", F.lit(None).cast("int")) \
        .withColumn("ProcessedBy", F.lit(None).cast("int")) \
        .withColumn("PromotionID", F.lit(None).cast("int")) \
        .withColumn("PromoCode", F.lit(None).cast("string")) \
        .withColumn("LoyaltyPointsEarned", (F.col("SubtotalAmount")/1).cast("int")) \
        .withColumn("CreatedDate", F.current_timestamp()) \
        .select("OrderID","OrderGUID","LocationID","ChannelID","MemberID","OrderDateID","OrderDateTime","OrderStatus","OrderType","SubtotalAmount","TaxAmount","DiscountAmount","DeliveryFee","TipAmount","TotalAmount","PaymentMethod","PaymentStatus","PaymentDateID","PreparedBy","ProcessedBy","PromotionID","PromoCode","LoyaltyPointsEarned","CreatedDate")

    # Join to calendar to get OrderDate and year for partitioning
    cal_small = cal.withColumnRenamed("CalendarID","C_CalendarID").withColumnRenamed("CalendarDate","OrderDate")
    orders = orders.join(cal_small, orders.OrderDateID == F.col("C_CalendarID")).drop("C_CALENDARID" if False else "C_CalendarID")
    # Write Orders partitioned by Year
    orders = orders.withColumn("Year", F.year("OrderDate"))
    write_parquet(orders, f"{output_root}/ord.Orders", partitionBy="Year")

    # ORDER ITEMS: explode 1..4 items per order deterministically
    items_small = items.cache()
    orders_for_items = orders.select("OrderID","OrderDateID","LocationID")
    orders_for_items = orders_for_items.withColumn("item_count", ((F.col("OrderID") % 4) + 1))
    orders_expl = orders_for_items.select("OrderID","LocationID","OrderDateID", F.expr("sequence(1,item_count) as seq")) \
                    .withColumn("seq", F.explode("seq"))
    # assign ItemID by pseudo-random mapping
    orders_expl = orders_expl.withColumn("ItemID", (((F.col("OrderID") * 131071) + F.col("seq")) % n_items + 1).cast("int"))
    # join prices
    order_items = orders_expl.join(items_small.withColumnRenamed("ItemID","I_ItemID"), orders_expl.ItemID == F.col("I_ItemID"), how="left") \
                  .withColumn("OrderItemID", F.monotonically_increasing_id()+1) \
                  .withColumn("Quantity", F.lit(1)) \
                  .withColumn("UnitPrice", F.col("BasePrice")) \
                  .withColumn("LineTotal", F.round(F.col("Quantity") * F.col("UnitPrice"),2)) \
                  .select("OrderItemID","OrderID","ItemID","I_ItemID","Quantity","UnitPrice","LineTotal") \
                  .withColumnRenamed("I_ItemID","ItemID")
    order_items = order_items.select("OrderItemID","OrderID","ItemID","Quantity","UnitPrice","LineTotal").withColumn("CreatedDate", F.current_timestamp())
    write_parquet(order_items, f"{output_root}/ord.OrderItems", partitionBy=None)

    # ORDER ITEM CUSTOMIZATIONS: small sample - none for most
    # PAYMENTS: one payment per order
    payments = orders.select("OrderID","OrderDateID","TotalAmount","PaymentMethod","PaymentStatus") \
             .withColumn("PaymentTransactionID", F.monotonically_increasing_id()+1) \
             .withColumn("PaymentDateID", F.col("OrderDateID")) \
             .withColumn("PaymentTime", F.current_timestamp()) \
             .withColumn("TransactionType", F.lit("Payment")) \
             .withColumn("TransactionStatus", F.lit("Success")) \
             .withColumn("ProcessedBy", F.lit(None).cast("int")) \
             .withColumn("CreatedDate", F.current_timestamp()) \
             .select("PaymentTransactionID","OrderID","PaymentDateID","PaymentTime","PaymentMethod","TotalAmount","TransactionType","TransactionStatus","ProcessedBy","CreatedDate")
    write_parquet(payments, f"{output_root}/ord.PaymentTransactions")

    # RECEIPTS
    receipts = orders.select("OrderID","OrderDateID").withColumn("ReceiptID", F.monotonically_increasing_id()+1) \
              .withColumn("ReceiptDateID", F.col("OrderDateID")).withColumn("PrintedTimestamp", F.current_timestamp()) \
              .withColumn("IsReprint", F.lit(0)).withColumn("CreatedDate", F.current_timestamp())
    write_parquet(receipts, f"{output_root}/ord.Receipts")

    # ORDER FEEDBACK: sample 1% of orders
    feedback = orders.sample(0.01).select("OrderID","MemberID").withColumn("FeedbackID", F.monotonically_increasing_id()+1) \
               .withColumn("OverallRating", (F.floor(F.rand()*5)+1).cast("int")) \
               .withColumn("CreatedDate", F.current_timestamp())
    write_parquet(feedback.select("FeedbackID","OrderID","MemberID","OverallRating","CreatedDate"), f"{output_root}/ord.OrderFeedback")

    # --- INVENTORY CONSUMPTION: create inventory transactions that deduct items from store inventory
    # Map menu item -> recipe ingredients -> inventory item IDs (we seeded inv.Items aligned with menu.Ingredients earlier)
    rec = spark.read.parquet(f"{output_root}/menu.RecipeItems").select("ItemID","IngredientID","Quantity")
    inv_items = spark.read.parquet(f"{output_root}/inv.Items").select("InventoryItemID","ItemName").withColumnRenamed("ItemName","InvItemName")
    # Join order_items -> recipe -> inventory
    oi = order_items.select("OrderItemID","OrderID","ItemID","Quantity").cache()
    oi_rec = oi.join(rec, "ItemID", how="left")
    # map ingredient -> inventory item using InventoryItemID ~ IngredientID mapping used earlier
    # our inv.Items used "LinkedIngredientID" mapping in creation; to be safe join on name if available, else map by ingredient id = inventory id
    # For simplicity assume InventoryItemID == IngredientID (because we created inv.Items from Ingredients)
    oi_rec = oi_rec.withColumn("InventoryItemID", F.col("IngredientID")) \
                  .withColumn("ConsumeQty", F.col("Quantity") * F.col("Quantity"))  # menu qty * recipe qty (simple)
    inv_txn = oi_rec.withColumn("TransactionID", F.monotonically_increasing_id()+1) \
                    .withColumn("StoreInventoryID", (F.col("OrderID")*100000 + F.col("InventoryItemID")).cast("long")) \
                    .withColumn("TransactionType", F.lit("Usage")) \
                    .withColumn("QuantityBefore", F.lit(100.0)) \
                    .withColumn("QuantityAfter", F.expr("QuantityBefore - ConsumeQty")) \
                    .withColumn("OrderIDRef", F.col("OrderID")) \
                    .withColumn("ReasonCode", F.lit("Sale")) \
                    .withColumn("TransactionDateID", F.col("OrderDateID")) \
                    .withColumn("ProcessedBy", F.lit(None).cast("int")) \
                    .select("TransactionID","StoreInventoryID","TransactionType","ConsumeQty","QuantityBefore","QuantityAfter","OrderIDRef","ReasonCode","TransactionDateID","ProcessedBy")
    inv_txn = inv_txn.withColumnRenamed("ConsumeQty","Quantity")
    write_parquet(inv_txn, f"{output_root}/inv.InventoryTransactions", partitionBy=None)

    # FINANCE LEDGER: create revenue, tax, payment entries per order (mirrors orders)
    ledger = orders.select("OrderID","OrderDateID","LocationID","SubtotalAmount","TaxAmount","TotalAmount") \
             .withColumn("LedgerID", F.monotonically_increasing_id()+1) \
             .withColumn("EntryType","Revenue") \
             .withColumn("Amount", F.col("TotalAmount")) \
             .withColumn("GLAccount", F.lit("4000")) \
             .withColumn("CreatedDate", F.current_timestamp()) \
             .select("LedgerID","OrderDateID","LocationID","OrderID","EntryType","Amount","GLAccount","CreatedDate")
    write_parquet(ledger, f"{output_root}/finance.Ledger", partitionBy=None)

    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    total = int(sys.argv[2]) if len(sys.argv)>2 else 5000000
    main(out, total)
