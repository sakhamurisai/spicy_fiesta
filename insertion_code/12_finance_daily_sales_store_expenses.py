"""Finance daily sales and store expenses generation module."""
from utils import get_spark, write_parquet
from azure_config import configure_azure_blob_storage, get_azure_blob_path
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)


def main():
    """Generate finance tables."""
    spark = get_spark("finance_agg")
    configure_azure_blob_storage(spark)
    
    try:
        # Get Azure paths
        azure_ord_path = get_azure_blob_path("ord")
        azure_dim_path = get_azure_blob_path("dim")
        azure_store_path = get_azure_blob_path("store")
        azure_finance_path = get_azure_blob_path("finance")
        
        # Read from Azure
        orders = spark.read.parquet(azure_ord_path).select("OrderID","LocationID","OrderDateID","TotalAmount")
        cal = spark.read.parquet(azure_dim_path).select("CalendarID","CalendarDate","Year")
        
        # Join and aggregate
        o_with_date = orders.join(cal.withColumnRenamed("CalendarID","C_CalID"), orders.OrderDateID == F.col("C_CalID")).drop("C_CalID")
        sales = o_with_date.groupBy("LocationID","OrderDateID","CalendarDate").agg(
            F.count("OrderID").alias("TotalOrders"), 
            F.sum("TotalAmount").alias("GrossSales")
        ) \
        .withColumn("TotalCustomers", F.col("TotalOrders")) \
        .withColumn("Discounts", F.lit(0.0)) \
        .withColumn("Refunds", F.lit(0.0)) \
        .withColumn("NetSales", F.col("GrossSales")) \
        .withColumn("TaxCollected", F.round(F.col("GrossSales")*0.07,2)) \
        .withColumn("InStoreSales", F.lit(0.0)) \
        .withColumn("DriveThruSales", F.lit(0.0)) \
        .withColumn("OnlineSales", F.lit(0.0)) \
        .withColumn("DeliverySales", F.lit(0.0)) \
        .withColumn("CashPayments", F.lit(0.0)) \
        .withColumn("CardPayments", F.lit(0.0)) \
        .withColumn("MobilePayments", F.lit(0.0)) \
        .withColumn("TotalLaborHours", F.lit(0.0)) \
        .withColumn("TotalLaborCost", F.lit(0.0)) \
        .withColumn("CostOfGoodsSold", F.lit(0.0)) \
        .withColumn("AverageOrderValue", F.expr("GrossSales/TotalOrders")) \
        .withColumn("AverageItemsPerOrder", F.lit(1.0)) \
        .withColumn("IsReconciled", F.lit(0)) \
        .withColumn("ReconciledBy", F.lit(None).cast("int")) \
        .withColumn("ReconciledDateID", F.lit(None).cast("int")) \
        .withColumn("CreatedDate", F.current_timestamp())
        
        sales = sales.withColumn("Year", F.year("CalendarDate"))
        write_parquet(sales, azure_finance_path, partitionBy="Year")
        
        # Generate store expenses
        locs = spark.read.parquet(azure_store_path).select("LocationID").limit(200)
        exp = []
        for l in locs.collect():
            exp.append((int(l.LocationID), 20050, "Rent", "Monthly rent", 5000.0, "Landlord Inc", "INV-1001", "Bank Transfer", None, int(1)))
        
        exp_df = spark.createDataFrame(exp, ["LocationID","ExpenseDateID","ExpenseCategory","ExpenseDescription","Amount","VendorName","InvoiceNumber","PaymentMethod","ApprovedBy","RecordedBy"]) \
                .withColumn("ExpenseID", F.monotonically_increasing_id()+1) \
                .withColumn("CreatedDate", F.current_timestamp())
        write_parquet(exp_df.select("ExpenseID","LocationID","ExpenseDateID","ExpenseCategory","ExpenseDescription","Amount","VendorName","InvoiceNumber","PaymentMethod","ApprovedBy","RecordedBy","CreatedDate"),
                      azure_finance_path)
        
        logger.info("Finance tables created successfully")
        
    except Exception as e:
        logger.error(f"Error generating finance: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()