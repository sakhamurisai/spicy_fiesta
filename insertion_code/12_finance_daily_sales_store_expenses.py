# 12_finance_daily_sales_store_expenses.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("finance_agg")
    orders = spark.read.parquet(f"{output_root}/ord.Orders").select("OrderID","LocationID","OrderDateID","TotalAmount")
    cal = spark.read.parquet(f"{output_root}/dim.Calendar").select("CalendarID","CalendarDate","Year")
    o_with_date = orders.join(cal.withColumnRenamed("CalendarID","C_CalID"), orders.OrderDateID == F.col("C_CalID")).drop("C_CalID")
    sales = o_with_date.groupBy("LocationID","OrderDateID","CalendarDate").agg(F.count("OrderID").alias("TotalOrders"), F.sum("TotalAmount").alias("GrossSales")) \
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
    # partition by year
    sales = sales.withColumn("Year", F.year("CalendarDate"))
    write_parquet(sales, f"{output_root}/finance.DailySalesSummary", partitionBy="Year")

    # StoreExpenses: sample expenses for stores
    locs = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID").limit(200)
    exp = []
    for l in locs.collect():
        exp.append((int(l.LocationID), 20050, "Rent", "Monthly rent", 5000.0, "Landlord Inc", "INV-1001", "Bank Transfer", None, int(1)))
    exp_df = spark.createDataFrame(exp, ["LocationID","ExpenseDateID","ExpenseCategory","ExpenseDescription","Amount","VendorName","InvoiceNumber","PaymentMethod","ApprovedBy","RecordedBy"]) \
            .withColumn("ExpenseID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp())
    write_parquet(exp_df.select("ExpenseID","LocationID","ExpenseDateID","ExpenseCategory","ExpenseDescription","Amount","VendorName","InvoiceNumber","PaymentMethod","ApprovedBy","RecordedBy","CreatedDate"),
                  f"{output_root}/finance.StoreExpenses")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
