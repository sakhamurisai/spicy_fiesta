"""
DATA PREPARATION - Spicy Fiesta Restaurant
Loads data from YOUR actual database schema and prepares for ML pipeline
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SpicyFiestaDataPreparation:
    """Load and prepare data from Spicy Fiesta database"""
    
    def __init__(self, spark, server, database, username, password):
        self.spark = spark
        self.jdbc_url = f"jdbc:sqlserver://{server}.database.windows.net:1433;database={database}"
        self.connection_properties = {
            "user": username,
            "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }
        logger.info(f"✅ Database connection configured: {database}")
    
    def load_customers(self):
        """Load customer data from loyalty.Customers"""
        
        logger.info("Loading customers from loyalty.Customers...")
        
        query = """
        (SELECT 
            c.CustomerID as customer_id,
            c.FirstName,
            c.LastName,
            c.Email,
            c.PhoneNumber,
            c.EnrollmentDateID,
            cal_enroll.CalendarDate as registration_date,
            c.CustomerTier,
            c.PointsBalance,
            c.LifetimeSpend,
            c.IsActive
        FROM loyalty.Customers c
        LEFT JOIN dim.Calendar cal_enroll ON c.EnrollmentDateID = cal_enroll.CalendarID
        WHERE c.IsActive = 1
        ) as customers
        """
        
        customers_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        )
        
        # Transform to expected schema
        customers_final = customers_df.select(
            col("customer_id").cast("long"),
            col("registration_date").cast("timestamp")
        )
        
        logger.info(f"✅ Loaded {customers_final.count()} customers")
        return customers_final
    
    def load_orders(self):
        """Load order data from ord.Orders"""
        
        logger.info("Loading orders from ord.Orders...")
        
        query = """
        (SELECT 
            o.OrderID as order_id,
            o.CustomerID as customer_id,
            cal.CalendarDate as order_date,
            CAST(cal.CalendarDate AS DATETIME2) + CAST(o.OrderTime AS DATETIME2) as order_timestamp,
            oc.ChannelName as channel,
            o.SubTotal,
            o.Tax,
            o.Discount,
            o.TotalAmount as order_total,
            o.OrderStatus,
            cal.DayName as day_of_week,
            cal.IsWeekend as is_weekend,
            DATEPART(HOUR, o.OrderTime) as hour_of_day
        FROM ord.Orders o
        INNER JOIN dim.Calendar cal ON o.OrderDateID = cal.CalendarID
        LEFT JOIN ord.OrderChannels oc ON o.OrderChannelID = oc.ChannelID
        WHERE o.OrderStatus IN ('Completed', 'Paid')
            AND cal.CalendarDate >= '2023-01-01'
            AND o.CustomerID IS NOT NULL
        ) as orders
        """
        
        orders_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        )
        
        # Transform to expected schema
        orders_final = orders_df.select(
            col("order_id").cast("long"),
            col("customer_id").cast("long"),
            col("order_timestamp").cast("timestamp"),
            col("channel"),
            col("order_total").cast("double"),
            col("day_of_week"),
            col("hour_of_day").cast("int"),
            col("is_weekend").cast("int")
        )
        
        logger.info(f"✅ Loaded {orders_final.count()} orders")
        return orders_final
    
    def load_order_items(self):
        """Load order line items from ord.OrderItems"""
        
        logger.info("Loading order items from ord.OrderItems...")
        
        query = """
        (SELECT 
            oi.OrderItemID as order_item_id,
            oi.OrderID as order_id,
            oi.MenuItemID as item_id,
            oi.Quantity as quantity,
            oi.UnitPrice as unit_price,
            oi.LineTotal as line_total
        FROM ord.OrderItems oi
        INNER JOIN ord.Orders o ON oi.OrderID = o.OrderID
        WHERE o.OrderStatus IN ('Completed', 'Paid')
            AND o.CustomerID IS NOT NULL
        ) as order_items
        """
        
        order_items_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        )
        
        # Transform to expected schema
        order_items_final = order_items_df.select(
            col("order_item_id").cast("long"),
            col("order_id").cast("long"),
            col("item_id").cast("int"),
            col("quantity").cast("int"),
            col("unit_price").cast("double"),
            col("line_total").cast("double")
        )
        
        logger.info(f"✅ Loaded {order_items_final.count()} order items")
        return order_items_final
    
    def load_menu_items(self):
        """Load menu items from menu.MenuItems"""
        
        logger.info("Loading menu items from menu.MenuItems...")
        
        query = """
        (SELECT 
            mi.MenuItemID as item_id,
            mi.ItemName as item_name,
            mc.CategoryName as category,
            mi.BasePrice as price,
            ISNULL(mi.SpiceLevel, 4) as spice_level
        FROM menu.MenuItems mi
        LEFT JOIN menu.Categories mc ON mi.CategoryID = mc.CategoryID
        WHERE mi.IsActive = 1
        ) as menu_items
        """
        
        items_df = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        )
        
        # Transform to expected schema
        items_final = items_df.select(
            col("item_id").cast("int"),
            col("item_name"),
            col("category"),
            col("spice_level").cast("int"),
            col("price").cast("double")
        )
        
        logger.info(f"✅ Loaded {items_final.count()} menu items")
        return items_final
    
    def enrich_with_customer_info(self, orders_df):
        """Join orders with full customer information"""
        
        logger.info("Enriching orders with customer details...")
        
        query = """
        (SELECT 
            CustomerID as customer_id,
            CustomerTier,
            PointsBalance,
            LifetimeSpend
        FROM loyalty.Customers
        WHERE IsActive = 1
        ) as customer_details
        """
        
        customer_details = self.spark.read.jdbc(
            url=self.jdbc_url,
            table=query,
            properties=self.connection_properties
        )
        
        enriched_df = orders_df.join(
            customer_details,
            "customer_id",
            "left"
        )
        
        return enriched_df
    
    def validate_data(self, customers_df, orders_df, order_items_df, items_df):
        """Validate loaded data quality"""
        
        logger.info("Validating data quality...")
        
        issues = []
        
        # Check counts
        customer_count = customers_df.count()
        order_count = orders_df.count()
        item_count = order_items_df.count()
        menu_count = items_df.count()
        
        if customer_count == 0:
            issues.append("❌ No customers found")
        if order_count == 0:
            issues.append("❌ No orders found")
        if item_count == 0:
            issues.append("❌ No order items found")
        if menu_count == 0:
            issues.append("❌ No menu items found")
        
        # Check for nulls in critical columns
        null_customers = orders_df.filter(col("customer_id").isNull()).count()
        if null_customers > 0:
            issues.append(f"⚠️  {null_customers} orders with null customer_id")
        
        null_totals = orders_df.filter(col("order_total").isNull()).count()
        if null_totals > 0:
            issues.append(f"⚠️  {null_totals} orders with null order_total")
        
        # Check relationships
        unique_customers_in_orders = orders_df.select("customer_id").distinct().count()
        
        logger.info(f"📊 Data Statistics:")
        logger.info(f"  Customers: {customer_count:,}")
        logger.info(f"  Orders: {order_count:,}")
        logger.info(f"  Order Items: {item_count:,}")
        logger.info(f"  Menu Items: {menu_count}")
        logger.info(f"  Customers with orders: {unique_customers_in_orders:,}")
        
        # Calculate date range
        date_range = orders_df.agg(
            min("order_timestamp").alias("min_date"),
            max("order_timestamp").alias("max_date")
        ).collect()[0]
        
        logger.info(f"  Date Range: {date_range['min_date']} to {date_range['max_date']}")
        
        # Print issues
        if issues:
            logger.warning("⚠️  Data validation issues:")
            for issue in issues:
                logger.warning(f"  {issue}")
        else:
            logger.info("✅ Data validation passed!")
        
        return len(issues) == 0
    
    def prepare_all_data(self):
        """Load and prepare all data for ML pipeline"""
        
        logger.info("="*80)
        logger.info("SPICY FIESTA - DATA PREPARATION")
        logger.info("="*80)
        
        # Load all tables
        customers_df = self.load_customers()
        orders_df = self.load_orders()
        order_items_df = self.load_order_items()
        items_df = self.load_menu_items()
        
        # Validate
        self.validate_data(customers_df, orders_df, order_items_df, items_df)
        
        # Create data dictionary
        data_dict = {
            "customers": customers_df,
            "transactions": orders_df,
            "order_items": order_items_df,
            "items": items_df
        }
        
        logger.info("="*80)
        logger.info("✅ DATA PREPARATION COMPLETE")
        logger.info("="*80)
        
        return data_dict
    
    def save_to_parquet(self, data_dict, output_path):
        """Save prepared data to parquet"""
        
        logger.info(f"Saving data to {output_path}...")
        
        for name, df in data_dict.items():
            path = f"{output_path}/{name}"
            
            # Repartition for optimal file size
            num_partitions = max(1, df.count() // 100000)
            
            df.repartition(num_partitions) \
                .write \
                .mode("overwrite") \
                .parquet(path)
            
            logger.info(f"✅ Saved {name} to {path}")
        
        logger.info("✅ All data saved to parquet!")


def main():
    """Main execution"""
    
    # ========================================================================
    # CONFIGURATION - UPDATE THESE VALUES
    # ========================================================================
    
    SERVER = "your-server-name"          # ← YOUR Azure SQL server
    DATABASE = "SpicyFiestaDB"           # ← YOUR database name
    USERNAME = "your-username"           # ← YOUR username
    PASSWORD = "your-password"           # ← YOUR password
    
    # Or use Databricks secrets (RECOMMENDED)
    # SERVER = dbutils.secrets.get(scope="azure-sql", key="server-name")
    # DATABASE = dbutils.secrets.get(scope="azure-sql", key="database-name")
    # USERNAME = dbutils.secrets.get(scope="azure-sql", key="username")
    # PASSWORD = dbutils.secrets.get(scope="azure-sql", key="password")
    
    OUTPUT_PATH = "/mnt/ml_data"  # or /dbfs/mnt/ml_data or /home/claude/ml_data
    
    # ========================================================================
    # EXECUTE DATA PREPARATION
    # ========================================================================
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("SpicyFiesta-Data-Preparation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Initialize data preparation
        data_prep = SpicyFiestaDataPreparation(
            spark=spark,
            server=SERVER,
            database=DATABASE,
            username=USERNAME,
            password=PASSWORD
        )
        
        # Load and prepare all data
        data_dict = data_prep.prepare_all_data()
        
        # Display samples
        print("\n" + "="*80)
        print("SAMPLE DATA")
        print("="*80)
        
        print("\n📊 Sample Customers:")
        data_dict["customers"].show(5, truncate=False)
        
        print("\n📊 Sample Orders:")
        data_dict["transactions"].show(5, truncate=False)
        
        print("\n📊 Sample Order Items:")
        data_dict["order_items"].show(5, truncate=False)
        
        print("\n📊 Sample Menu Items:")
        data_dict["items"].show(5, truncate=False)
        
        # Save to parquet
        data_prep.save_to_parquet(data_dict, OUTPUT_PATH)
        
        # Final statistics
        print("\n" + "="*80)
        print("DATA PREPARATION SUMMARY")
        print("="*80)
        print(f"✅ Customers: {data_dict['customers'].count():,}")
        print(f"✅ Orders: {data_dict['transactions'].count():,}")
        print(f"✅ Order Items: {data_dict['order_items'].count():,}")
        print(f"✅ Menu Items: {data_dict['items'].count():,}")
        print(f"✅ Data saved to: {OUTPUT_PATH}")
        print("="*80)
        print("\n📍 NEXT STEP: Run feature engineering")
        print("   → python 02_feature_engineering.py")
        print("="*80)
        
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
