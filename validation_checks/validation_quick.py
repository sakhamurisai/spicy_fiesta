"""
Quick Validation Checks - Run this for a fast health check
"""

from pyspark.sql import SparkSession
from datetime import datetime

print("=" * 80)
print("QUICK VALIDATION CHECKS")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

spark = SparkSession.builder.appName("QuickValidation").getOrCreate()
MOUNT_POINT = "/mnt/restaurant_input"

# Track results
results = {"passed": 0, "failed": 0, "total": 0}

def check(description, condition, error_msg=""):
    """Quick check function"""
    results["total"] += 1
    try:
        if condition:
            print(f"✅ {description}")
            results["passed"] += 1
            return True
        else:
            print(f"❌ {description} - {error_msg}")
            results["failed"] += 1
            return False
    except Exception as e:
        print(f"❌ {description} - Error: {str(e)}")
        results["failed"] += 1
        return False

print("\n--- PHASE 1: FOLDER EXISTENCE ---")
schemas = ["dim", "store", "emp", "menu", "inv", "promo", "loyalty", "ord", "finance", "dbo"]
for schema in schemas:
    try:
        dbutils.fs.ls(f"{MOUNT_POINT}/{schema}")
        check(f"Folder exists: {schema}", True)
    except:
        check(f"Folder exists: {schema}", False, "Folder not found")

print("\n--- PHASE 2: ROW COUNTS ---")

# Calendar
try:
    cal_count = spark.read.parquet(f"{MOUNT_POINT}/dim").count()
    check(f"Calendar: {cal_count:,} rows", cal_count >= 30000, f"Expected >= 30,000")
except Exception as e:
    check("Calendar row count", False, str(e))

# States
try:
    store_df = spark.read.parquet(f"{MOUNT_POINT}/store")
    states_count = store_df.filter("StateID IS NOT NULL").select("StateID").distinct().count()
    check(f"States: {states_count} rows", states_count == 51, f"Expected 51 states")
except Exception as e:
    check("States row count", False, str(e))

# Locations
try:
    locs_count = store_df.filter("LocationID IS NOT NULL").select("LocationID").distinct().count()
    check(f"Locations: {locs_count} rows", locs_count >= 100, f"Expected >= 100")
except Exception as e:
    check("Locations row count", False, str(e))

# Employees
try:
    emp_df = spark.read.parquet(f"{MOUNT_POINT}/emp")
    emp_count = emp_df.filter("EmployeeID IS NOT NULL").select("EmployeeID").distinct().count()
    check(f"Employees: {emp_count} rows", emp_count >= 500, f"Expected >= 500")
except Exception as e:
    check("Employees row count", False, str(e))

# Menu Items
try:
    menu_df = spark.read.parquet(f"{MOUNT_POINT}/menu")
    items_count = menu_df.filter("ItemID IS NOT NULL").select("ItemID").distinct().count()
    check(f"Menu Items: {items_count} rows", items_count >= 200, f"Expected >= 200")
except Exception as e:
    check("Menu Items row count", False, str(e))

# Orders (CRITICAL)
try:
    ord_df = spark.read.parquet(f"{MOUNT_POINT}/ord")
    orders_count = ord_df.filter("OrderID IS NOT NULL").select("OrderID").distinct().count()
    check(f"Orders: {orders_count:,} rows", orders_count >= 100000, f"Expected >= 100,000")
    
    # Order Items
    items_count = ord_df.filter("OrderItemID IS NOT NULL").select("OrderItemID").distinct().count()
    check(f"Order Items: {items_count:,} rows", items_count >= 200000, f"Expected >= 200,000")
except Exception as e:
    check("Orders row count", False, str(e))

# Finance
try:
    finance_df = spark.read.parquet(f"{MOUNT_POINT}/finance")
    finance_count = finance_df.count()
    check(f"Finance: {finance_count:,} rows", finance_count >= 100, f"Expected >= 100")
except Exception as e:
    check("Finance row count", False, str(e))

print("\n--- PHASE 3: DATA QUALITY SPOT CHECKS ---")

# Check for nulls in critical columns
try:
    null_orders = ord_df.filter("OrderID IS NULL").count()
    check("Orders: No null OrderIDs", null_orders == 0, f"Found {null_orders} nulls")
except Exception as e:
    check("Orders null check", False, str(e))

try:
    null_amounts = ord_df.filter("TotalAmount IS NULL OR TotalAmount <= 0").count()
    check("Orders: Valid TotalAmount", null_amounts == 0, f"Found {null_amounts} invalid amounts")
except Exception as e:
    check("Orders amount check", False, str(e))

# Check for duplicates
try:
    total_orders = ord_df.filter("OrderID IS NOT NULL").count()
    distinct_orders = ord_df.filter("OrderID IS NOT NULL").select("OrderID").distinct().count()
    check("Orders: No duplicate OrderIDs", total_orders == distinct_orders, 
          f"{total_orders - distinct_orders} duplicates found")
except Exception as e:
    check("Orders duplicate check", False, str(e))

print("\n--- PHASE 4: BUSINESS RULE CHECKS ---")

# Orders: TotalAmount >= SubtotalAmount
try:
    invalid_totals = ord_df.filter("TotalAmount < SubtotalAmount").count()
    check("Orders: TotalAmount >= SubtotalAmount", invalid_totals == 0,
          f"Found {invalid_totals} violations")
except Exception as e:
    check("Orders total validation", False, str(e))

# Menu: Prices > 0
try:
    invalid_prices = menu_df.filter("BasePrice <= 0").count()
    check("Menu: BasePrice > 0", invalid_prices == 0, f"Found {invalid_prices} invalid prices")
except Exception as e:
    check("Menu price validation", False, str(e))

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(f"Total Checks: {results['total']}")
print(f"✅ Passed: {results['passed']} ({results['passed']/results['total']*100:.1f}%)")
print(f"❌ Failed: {results['failed']} ({results['failed']/results['total']*100:.1f}%)")

if results['failed'] == 0:
    print("\n🎉 ALL CHECKS PASSED! Your data looks good!")
else:
    print(f"\n⚠️  {results['failed']} checks failed. Run validation_comprehensive.py for details.")

print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
