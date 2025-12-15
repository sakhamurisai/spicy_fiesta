"""
Comprehensive Validation Suite for Spicy Fiesta Data Pipeline
Run this after pipeline execution to verify data quality and completeness
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as _sum, avg, min as _min, max as _max, isnan, isnull
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Azure configuration
MOUNT_POINT = "/mnt/restaurant_input"

print("=" * 100)
print("SPICY FIESTA DATA PIPELINE - COMPREHENSIVE VALIDATION SUITE")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)

# Get Spark session
spark = SparkSession.builder.appName("Validation").getOrCreate()

# Validation results tracker
validation_results = {
    "passed": [],
    "failed": [],
    "warnings": []
}


def log_result(status, table, check, details=""):
    """Log validation result"""
    result = {
        "table": table,
        "check": check,
        "details": details,
        "timestamp": datetime.now()
    }
    
    if status == "PASS":
        validation_results["passed"].append(result)
        logger.info(f"✅ PASS - {table}: {check} {details}")
    elif status == "FAIL":
        validation_results["failed"].append(result)
        logger.error(f"❌ FAIL - {table}: {check} {details}")
    else:  # WARNING
        validation_results["warnings"].append(result)
        logger.warning(f"⚠️  WARN - {table}: {check} {details}")


def validate_folder_exists(schema_name):
    """Check if output folder exists"""
    try:
        path = f"{MOUNT_POINT}/{schema_name}"
        files = dbutils.fs.ls(path)
        log_result("PASS", schema_name, "Folder exists", f"({len(files)} files/partitions)")
        return True
    except Exception as e:
        log_result("FAIL", schema_name, "Folder exists", f"Error: {str(e)}")
        return False


def validate_table(schema_name, expected_min_count=0, expected_max_count=None):
    """Basic table validation - existence and row count"""
    try:
        path = f"{MOUNT_POINT}/{schema_name}"
        df = spark.read.parquet(path)
        
        # Get count
        actual_count = df.count()
        
        # Check minimum count
        if actual_count < expected_min_count:
            log_result("FAIL", schema_name, "Row count", 
                      f"Expected >={expected_min_count}, got {actual_count}")
        else:
            log_result("PASS", schema_name, "Row count", f"{actual_count:,} rows")
        
        # Check maximum count if specified
        if expected_max_count and actual_count > expected_max_count:
            log_result("WARNING", schema_name, "Row count", 
                      f"Exceeds expected max {expected_max_count:,}, got {actual_count:,}")
        
        return df, actual_count
        
    except Exception as e:
        log_result("FAIL", schema_name, "Read table", f"Error: {str(e)}")
        return None, 0


def validate_schema(df, table_name, required_columns):
    """Validate schema has required columns"""
    if df is None:
        return False
    
    actual_columns = set(df.columns)
    required_set = set(required_columns)
    
    missing = required_set - actual_columns
    extra = actual_columns - required_set
    
    if missing:
        log_result("FAIL", table_name, "Schema validation", f"Missing columns: {missing}")
        return False
    else:
        log_result("PASS", table_name, "Schema validation", f"All {len(required_columns)} required columns present")
    
    if extra:
        log_result("WARNING", table_name, "Schema validation", f"Extra columns: {extra}")
    
    return True


def validate_nulls(df, table_name, non_null_columns):
    """Check for nulls in columns that shouldn't have them"""
    if df is None:
        return
    
    for col_name in non_null_columns:
        try:
            null_count = df.filter(col(col_name).isNull()).count()
            if null_count > 0:
                log_result("FAIL", table_name, f"Null check - {col_name}", 
                          f"{null_count} null values found")
            else:
                log_result("PASS", table_name, f"Null check - {col_name}", "No nulls")
        except Exception as e:
            log_result("WARNING", table_name, f"Null check - {col_name}", f"Error: {str(e)}")


def validate_duplicates(df, table_name, unique_columns):
    """Check for duplicates in columns that should be unique"""
    if df is None:
        return
    
    try:
        total_count = df.count()
        distinct_count = df.select(unique_columns).distinct().count()
        
        if total_count != distinct_count:
            duplicates = total_count - distinct_count
            log_result("FAIL", table_name, f"Duplicate check - {unique_columns}", 
                      f"{duplicates} duplicate(s) found")
        else:
            log_result("PASS", table_name, f"Duplicate check - {unique_columns}", "No duplicates")
    except Exception as e:
        log_result("WARNING", table_name, f"Duplicate check", f"Error: {str(e)}")


def validate_foreign_key(df, table_name, fk_column, parent_df, parent_table, pk_column):
    """Validate foreign key relationships"""
    if df is None or parent_df is None:
        return
    
    try:
        # Get distinct FKs
        fk_values = df.select(fk_column).distinct()
        
        # Get distinct PKs
        pk_values = parent_df.select(pk_column).distinct()
        
        # Left anti join to find orphaned records
        orphaned = fk_values.join(pk_values, fk_values[fk_column] == pk_values[pk_column], "left_anti")
        orphan_count = orphaned.count()
        
        if orphan_count > 0:
            log_result("FAIL", table_name, f"FK {fk_column} -> {parent_table}.{pk_column}", 
                      f"{orphan_count} orphaned records")
        else:
            log_result("PASS", table_name, f"FK {fk_column} -> {parent_table}.{pk_column}", 
                      "All FKs valid")
    except Exception as e:
        log_result("WARNING", table_name, f"FK validation", f"Error: {str(e)}")


def validate_date_range(df, table_name, date_column, min_date=None, max_date=None):
    """Validate date ranges"""
    if df is None:
        return
    
    try:
        df_min = df.agg(_min(date_column)).collect()[0][0]
        df_max = df.agg(_max(date_column)).collect()[0][0]
        
        result = f"Range: {df_min} to {df_max}"
        
        issues = []
        if min_date and df_min < min_date:
            issues.append(f"Min date {df_min} is before expected {min_date}")
        if max_date and df_max > max_date:
            issues.append(f"Max date {df_max} is after expected {max_date}")
        
        if issues:
            log_result("FAIL", table_name, f"Date range - {date_column}", ", ".join(issues))
        else:
            log_result("PASS", table_name, f"Date range - {date_column}", result)
    except Exception as e:
        log_result("WARNING", table_name, f"Date range validation", f"Error: {str(e)}")


def validate_numeric_range(df, table_name, column, min_val=None, max_val=None):
    """Validate numeric ranges"""
    if df is None:
        return
    
    try:
        df_min = df.agg(_min(column)).collect()[0][0]
        df_max = df.agg(_max(column)).collect()[0][0]
        
        issues = []
        if min_val is not None and df_min < min_val:
            issues.append(f"Min {df_min} < expected {min_val}")
        if max_val is not None and df_max > max_val:
            issues.append(f"Max {df_max} > expected {max_val}")
        
        if issues:
            log_result("FAIL", table_name, f"Range check - {column}", ", ".join(issues))
        else:
            log_result("PASS", table_name, f"Range check - {column}", 
                      f"Range: {df_min} to {df_max}")
    except Exception as e:
        log_result("WARNING", table_name, f"Range validation", f"Error: {str(e)}")


def validate_business_rules(df, table_name, rule_name, condition, expected_failures=0):
    """Validate business rules"""
    if df is None:
        return
    
    try:
        violations = df.filter(~condition).count()
        
        if violations > expected_failures:
            log_result("FAIL", table_name, f"Business rule - {rule_name}", 
                      f"{violations} violations (expected <={expected_failures})")
        else:
            log_result("PASS", table_name, f"Business rule - {rule_name}", 
                      f"{violations} violations")
    except Exception as e:
        log_result("WARNING", table_name, f"Business rule - {rule_name}", f"Error: {str(e)}")


print("\n" + "=" * 100)
print("PHASE 1: FOLDER EXISTENCE CHECKS")
print("=" * 100)

schemas = ["dim", "store", "emp", "menu", "inv", "promo", "loyalty", "ord", "finance", "dbo"]
for schema in schemas:
    validate_folder_exists(schema)


print("\n" + "=" * 100)
print("PHASE 2: CALENDAR DIMENSION VALIDATION")
print("=" * 100)

cal_df, cal_count = validate_table("dim", expected_min_count=30000, expected_max_count=40000)

if cal_df:
    # Schema validation
    cal_required_cols = ["CalendarID", "CalendarDate", "Year", "Quarter", "Month", 
                         "DayOfWeek", "IsWeekend", "FiscalYear"]
    validate_schema(cal_df, "dim/Calendar", cal_required_cols)
    
    # Null checks
    validate_nulls(cal_df, "dim/Calendar", ["CalendarID", "CalendarDate", "Year"])
    
    # Duplicate check
    validate_duplicates(cal_df, "dim/Calendar", "CalendarID")
    validate_duplicates(cal_df, "dim/Calendar", "CalendarDate")
    
    # Date range
    validate_date_range(cal_df, "dim/Calendar", "CalendarDate", "1980-01-01", "2080-12-31")
    
    # Business rules
    validate_business_rules(cal_df, "dim/Calendar", "Weekend flag matches DayOfWeek",
                           (col("IsWeekend") == 1) == (col("DayOfWeek").isin([6, 7])))
    
    validate_business_rules(cal_df, "dim/Calendar", "Valid quarters",
                           col("Quarter").between(1, 4))


print("\n" + "=" * 100)
print("PHASE 3: STORE SCHEMA VALIDATION")
print("=" * 100)

store_df, store_count = validate_table("store", expected_min_count=1500)

if store_df:
    # Separate different entity types in store schema
    states_df = store_df.filter(col("StateID").isNotNull()).select("StateID", "StateCode", "StateName", "TaxRate")
    locations_df = store_df.filter(col("LocationID").isNotNull()).select("LocationID", "StateID", "StoreNumber")
    
    # States validation
    states_count = states_df.count()
    if states_count == 51:
        log_result("PASS", "store/States", "State count", f"{states_count} states")
    else:
        log_result("FAIL", "store/States", "State count", f"Expected 51, got {states_count}")
    
    validate_duplicates(states_df, "store/States", "StateCode")
    validate_numeric_range(states_df, "store/States", "TaxRate", 0.0, 0.15)
    
    # Locations validation
    locs_count = locations_df.count()
    if locs_count >= 100:
        log_result("PASS", "store/Locations", "Location count", f"{locs_count} locations")
    else:
        log_result("FAIL", "store/Locations", "Location count", f"Expected >=100, got {locs_count}")
    
    validate_duplicates(locations_df, "store/Locations", "LocationID")
    validate_duplicates(locations_df, "store/Locations", "StoreNumber")
    
    # FK validation
    validate_foreign_key(locations_df, "store/Locations", "StateID", 
                        states_df, "store/States", "StateID")


print("\n" + "=" * 100)
print("PHASE 4: EMPLOYEE SCHEMA VALIDATION")
print("=" * 100)

emp_df, emp_count = validate_table("emp", expected_min_count=900)

if emp_df:
    # Positions
    positions_df = emp_df.filter(col("PositionID").isNotNull()).select("PositionID", "PositionCode", "MinHourlyRate", "MaxHourlyRate")
    pos_count = positions_df.count()
    
    if pos_count >= 4:
        log_result("PASS", "emp/Positions", "Position count", f"{pos_count} positions")
    else:
        log_result("FAIL", "emp/Positions", "Position count", f"Expected >=4, got {pos_count}")
    
    validate_duplicates(positions_df, "emp/Positions", "PositionID")
    validate_business_rules(positions_df, "emp/Positions", "Min rate < Max rate",
                           col("MinHourlyRate") <= col("MaxHourlyRate"))
    
    # Employees
    employees_df = emp_df.filter(col("EmployeeID").isNotNull()).select("EmployeeID", "EmployeeNumber", "PositionID", "PrimaryLocationID", "HourlyRate")
    emps_count = employees_df.count()
    
    if emps_count >= 500:
        log_result("PASS", "emp/Employees", "Employee count", f"{emps_count} employees")
    else:
        log_result("FAIL", "emp/Employees", "Employee count", f"Expected >=500, got {emps_count}")
    
    validate_duplicates(employees_df, "emp/Employees", "EmployeeID")
    validate_duplicates(employees_df, "emp/Employees", "EmployeeNumber")
    validate_numeric_range(employees_df, "emp/Employees", "HourlyRate", 10.0, 100.0)
    
    # FK validations
    if store_df:
        validate_foreign_key(employees_df, "emp/Employees", "PrimaryLocationID",
                           locations_df, "store/Locations", "LocationID")


print("\n" + "=" * 100)
print("PHASE 5: MENU SCHEMA VALIDATION")
print("=" * 100)

menu_df, menu_count = validate_table("menu", expected_min_count=250)

if menu_df:
    # Categories
    categories_df = menu_df.filter(col("CategoryID").isNotNull()).select("CategoryID", "CategoryCode", "CategoryName")
    cat_count = categories_df.count()
    
    if cat_count >= 4:
        log_result("PASS", "menu/Categories", "Category count", f"{cat_count} categories")
    else:
        log_result("FAIL", "menu/Categories", "Category count", f"Expected >=4, got {cat_count}")
    
    # Menu Items
    items_df = menu_df.filter(col("ItemID").isNotNull()).select("ItemID", "ItemCode", "CategoryID", "BasePrice")
    items_count = items_df.count()
    
    if items_count >= 200:
        log_result("PASS", "menu/Items", "Item count", f"{items_count} items")
    else:
        log_result("FAIL", "menu/Items", "Item count", f"Expected >=200, got {items_count}")
    
    validate_duplicates(items_df, "menu/Items", "ItemID")
    validate_duplicates(items_df, "menu/Items", "ItemCode")
    validate_numeric_range(items_df, "menu/Items", "BasePrice", 0.50, 50.0)
    
    # FK validation
    validate_foreign_key(items_df, "menu/Items", "CategoryID",
                        categories_df, "menu/Categories", "CategoryID")


print("\n" + "=" * 100)
print("PHASE 6: INVENTORY SCHEMA VALIDATION")
print("=" * 100)

inv_df, inv_count = validate_table("inv", expected_min_count=100)

if inv_df:
    # Inventory items
    inv_items_df = inv_df.filter(col("InventoryItemID").isNotNull()).select("InventoryItemID", "ItemCode", "UnitCost", "ReorderLevel")
    inv_items_count = inv_items_df.count()
    
    if inv_items_count >= 5:
        log_result("PASS", "inv/InventoryItems", "Item count", f"{inv_items_count} items")
    else:
        log_result("FAIL", "inv/InventoryItems", "Item count", f"Expected >=5, got {inv_items_count}")
    
    validate_duplicates(inv_items_df, "inv/InventoryItems", "InventoryItemID")
    validate_numeric_range(inv_items_df, "inv/InventoryItems", "UnitCost", 0.0, 1000.0)
    
    # Purchase Orders
    po_df = inv_df.filter(col("PurchaseOrderID").isNotNull()).select("PurchaseOrderID", "LocationID", "TotalAmount", "OrderStatus")
    po_count = po_df.count()
    
    if po_count >= 100:
        log_result("PASS", "inv/PurchaseOrders", "PO count", f"{po_count} purchase orders")
    else:
        log_result("WARNING", "inv/PurchaseOrders", "PO count", f"Expected >=100, got {po_count}")
    
    validate_duplicates(po_df, "inv/PurchaseOrders", "PurchaseOrderID")


print("\n" + "=" * 100)
print("PHASE 7: PROMOTIONS SCHEMA VALIDATION")
print("=" * 100)

promo_df, promo_count = validate_table("promo", expected_min_count=2)

if promo_df:
    # Promotions
    promotions_df = promo_df.filter(col("PromotionID").isNotNull()).select("PromotionID", "PromotionCode", "PromotionType", "DiscountPercentage", "DiscountAmount")
    promos_count = promotions_df.count()
    
    if promos_count >= 1:
        log_result("PASS", "promo/Promotions", "Promotion count", f"{promos_count} promotions")
    else:
        log_result("FAIL", "promo/Promotions", "Promotion count", f"Expected >=1, got {promos_count}")
    
    validate_duplicates(promotions_df, "promo/Promotions", "PromotionID")
    
    # Business rule: Either percentage or amount should be set
    validate_business_rules(promotions_df, "promo/Promotions", 
                           "Either percentage or amount set",
                           (col("DiscountPercentage").isNotNull()) | (col("DiscountAmount").isNotNull()))


print("\n" + "=" * 100)
print("PHASE 8: LOYALTY SCHEMA VALIDATION")
print("=" * 100)

loyalty_df, loyalty_count = validate_table("loyalty", expected_min_count=1000)

if loyalty_df:
    # Members
    members_df = loyalty_df.filter(col("MemberID").isNotNull()).select("MemberID", "MemberNumber", "Email", "TotalPoints")
    members_count = members_df.count()
    
    if members_count >= 1000:
        log_result("PASS", "loyalty/Members", "Member count", f"{members_count} members")
    else:
        log_result("WARNING", "loyalty/Members", "Member count", f"Expected >=1000, got {members_count}")
    
    validate_duplicates(members_df, "loyalty/Members", "MemberID")
    validate_duplicates(members_df, "loyalty/Members", "MemberNumber")
    validate_numeric_range(members_df, "loyalty/Members", "TotalPoints", 0, 1000000)


print("\n" + "=" * 100)
print("PHASE 9: ORDERS SCHEMA VALIDATION (CRITICAL)")
print("=" * 100)

ord_df, ord_count = validate_table("ord", expected_min_count=1000000)

if ord_df:
    # Orders
    orders_df = ord_df.filter(col("OrderID").isNotNull()).select("OrderID", "LocationID", "OrderDateID", "OrderStatus", "TotalAmount", "SubtotalAmount", "TaxAmount")
    orders_count = orders_df.count()
    
    if orders_count >= 1000000:
        log_result("PASS", "ord/Orders", "Order count", f"{orders_count:,} orders")
    else:
        log_result("WARNING", "ord/Orders", "Order count", f"Expected >=1M, got {orders_count:,}")
    
    validate_duplicates(orders_df, "ord/Orders", "OrderID")
    validate_numeric_range(orders_df, "ord/Orders", "TotalAmount", 0.0, 10000.0)
    validate_numeric_range(orders_df, "ord/Orders", "SubtotalAmount", 0.0, 10000.0)
    
    # Business rules
    validate_business_rules(orders_df, "ord/Orders", "TotalAmount >= SubtotalAmount",
                           col("TotalAmount") >= col("SubtotalAmount"))
    
    validate_business_rules(orders_df, "ord/Orders", "TaxAmount calculation (approximate)",
                           (col("TaxAmount") >= 0) & (col("TaxAmount") <= col("SubtotalAmount") * 0.15))
    
    # FK validations
    if cal_df:
        validate_foreign_key(orders_df, "ord/Orders", "OrderDateID",
                           cal_df, "dim/Calendar", "CalendarID")
    
    if store_df:
        validate_foreign_key(orders_df, "ord/Orders", "LocationID",
                           locations_df, "store/Locations", "LocationID")
    
    # Order Items
    order_items_df = ord_df.filter(col("OrderItemID").isNotNull()).select("OrderItemID", "OrderID", "ItemID", "Quantity", "UnitPrice", "LineTotal")
    items_count = order_items_df.count()
    
    expected_min_items = orders_count * 2  # Average 2+ items per order
    if items_count >= expected_min_items:
        log_result("PASS", "ord/OrderItems", "Item count", f"{items_count:,} order items")
    else:
        log_result("WARNING", "ord/OrderItems", "Item count", 
                  f"Expected >={expected_min_items:,}, got {items_count:,}")
    
    validate_business_rules(order_items_df, "ord/OrderItems", "LineTotal = Quantity * UnitPrice",
                           col("LineTotal") == (col("Quantity") * col("UnitPrice")))


print("\n" + "=" * 100)
print("PHASE 10: FINANCE SCHEMA VALIDATION")
print("=" * 100)

finance_df, finance_count = validate_table("finance", expected_min_count=100)

if finance_df:
    # Daily Sales Summary
    sales_df = finance_df.filter(col("TotalOrders").isNotNull()).select("LocationID", "OrderDateID", "TotalOrders", "GrossSales", "NetSales", "AverageOrderValue")
    sales_count = sales_df.count()
    
    if sales_count >= 100:
        log_result("PASS", "finance/DailySales", "Record count", f"{sales_count:,} daily summaries")
    else:
        log_result("WARNING", "finance/DailySales", "Record count", f"Expected >=100, got {sales_count}")
    
    validate_numeric_range(sales_df, "finance/DailySales", "TotalOrders", 1, 100000)
    validate_numeric_range(sales_df, "finance/DailySales", "GrossSales", 0.0, 10000000.0)
    
    # Business rules
    validate_business_rules(sales_df, "finance/DailySales", "NetSales <= GrossSales",
                           col("NetSales") <= col("GrossSales"))
    
    validate_business_rules(sales_df, "finance/DailySales", "AverageOrderValue calculation",
                           (col("TotalOrders") == 0) | 
                           ((col("AverageOrderValue") >= (col("GrossSales") / col("TotalOrders") - 0.1)) &
                            (col("AverageOrderValue") <= (col("GrossSales") / col("TotalOrders") + 0.1))))


print("\n" + "=" * 100)
print("PHASE 11: SYSTEM SCHEMA VALIDATION")
print("=" * 100)

dbo_df, dbo_count = validate_table("dbo", expected_min_count=1)

if dbo_df:
    config_df = dbo_df.filter(col("ConfigID").isNotNull()).select("ConfigID", "ConfigKey", "ConfigValue")
    config_count = config_df.count()
    
    if config_count >= 1:
        log_result("PASS", "dbo/SystemConfiguration", "Config count", f"{config_count} configs")
    else:
        log_result("WARNING", "dbo/SystemConfiguration", "Config count", f"Expected >=1, got {config_count}")


print("\n" + "=" * 100)
print("PHASE 12: CROSS-SCHEMA VALIDATIONS")
print("=" * 100)

# Validate that orders exist for multiple locations
if ord_df and store_df:
    orders_per_location = orders_df.groupBy("LocationID").count()
    locations_with_orders = orders_per_location.count()
    total_locations = locations_df.count()
    
    coverage_pct = (locations_with_orders / total_locations) * 100 if total_locations > 0 else 0
    
    if coverage_pct >= 80:
        log_result("PASS", "Cross-schema", "Location coverage in orders", 
                  f"{locations_with_orders}/{total_locations} locations ({coverage_pct:.1f}%)")
    else:
        log_result("WARNING", "Cross-schema", "Location coverage in orders",
                  f"Only {locations_with_orders}/{total_locations} locations ({coverage_pct:.1f}%) have orders")

# Validate date distribution in orders
if ord_df and cal_df:
    orders_per_date = orders_df.groupBy("OrderDateID").count()
    unique_dates = orders_per_date.count()
    
    if unique_dates >= 30:
        log_result("PASS", "Cross-schema", "Date distribution in orders",
                  f"Orders span {unique_dates} unique dates")
    else:
        log_result("WARNING", "Cross-schema", "Date distribution in orders",
                  f"Orders only span {unique_dates} dates (expected >30)")


print("\n" + "=" * 100)
print("VALIDATION SUMMARY")
print("=" * 100)

total_checks = len(validation_results["passed"]) + len(validation_results["failed"]) + len(validation_results["warnings"])
pass_count = len(validation_results["passed"])
fail_count = len(validation_results["failed"])
warn_count = len(validation_results["warnings"])

print(f"\nTotal Checks: {total_checks}")
print(f"✅ Passed: {pass_count} ({(pass_count/total_checks*100):.1f}%)")
print(f"❌ Failed: {fail_count} ({(fail_count/total_checks*100):.1f}%)")
print(f"⚠️  Warnings: {warn_count} ({(warn_count/total_checks*100):.1f}%)")

if fail_count > 0:
    print("\n" + "=" * 100)
    print("FAILED CHECKS - REQUIRE ATTENTION")
    print("=" * 100)
    for result in validation_results["failed"]:
        print(f"\n❌ {result['table']}: {result['check']}")
        print(f"   {result['details']}")

if warn_count > 0:
    print("\n" + "=" * 100)
    print("WARNINGS - REVIEW RECOMMENDED")
    print("=" * 100)
    for result in validation_results["warnings"]:
        print(f"\n⚠️  {result['table']}: {result['check']}")
        print(f"   {result['details']}")

print("\n" + "=" * 100)
if fail_count == 0:
    print("🎉 ALL CRITICAL VALIDATIONS PASSED!")
    print("Your data pipeline has generated high-quality data ready for use.")
else:
    print("⚠️  SOME VALIDATIONS FAILED")
    print("Please review the failed checks above and re-run affected pipeline steps.")

print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 100)
