# 📊 Data Validation Guide

## Overview

After running your pipeline, use these validation scripts to ensure data quality and completeness.

---

## 🚀 Quick Start - Run Validations

### **Option 1: Quick Health Check** ⭐ (2 minutes)

```python
%run ./validation_quick.py
```

**What it checks:**
- ✅ All 10 folders exist
- ✅ Row counts are within expected ranges
- ✅ No nulls in critical columns
- ✅ No duplicate primary keys
- ✅ Basic business rules

**Use when:** You want a fast sanity check after pipeline execution

---

### **Option 2: Comprehensive Validation** (10-15 minutes)

```python
%run ./validation_comprehensive.py
```

**What it checks:**
- ✅ Everything from quick check PLUS:
- ✅ Schema validation (all required columns)
- ✅ Foreign key integrity
- ✅ Date range validations
- ✅ Numeric range validations  
- ✅ Complex business rules
- ✅ Cross-schema validations

**Use when:** You need detailed validation results for production deployment

---

## 📋 Manual Validation Commands

If you prefer to check specific things manually:

### **1. Check All Folders Exist**

```python
folders = ["dim", "store", "emp", "menu", "inv", "promo", "loyalty", "ord", "finance", "dbo"]

for folder in folders:
    try:
        files = dbutils.fs.ls(f"/mnt/restaurant_input/{folder}")
        print(f"✅ {folder}: {len(files)} files/partitions")
    except Exception as e:
        print(f"❌ {folder}: NOT FOUND - {str(e)}")
```

---

### **2. Count Records in Each Table**

```python
tables = {
    "Calendar (dim)": "/mnt/restaurant_input/dim",
    "Store Data": "/mnt/restaurant_input/store",
    "Employee Data": "/mnt/restaurant_input/emp",
    "Menu Data": "/mnt/restaurant_input/menu",
    "Inventory": "/mnt/restaurant_input/inv",
    "Promotions": "/mnt/restaurant_input/promo",
    "Loyalty": "/mnt/restaurant_input/loyalty",
    "Orders": "/mnt/restaurant_input/ord",
    "Finance": "/mnt/restaurant_input/finance",
    "System": "/mnt/restaurant_input/dbo"
}

for name, path in tables.items():
    try:
        df = spark.read.parquet(path)
        count = df.count()
        print(f"✅ {name}: {count:,} records")
    except Exception as e:
        print(f"❌ {name}: Error - {str(e)}")
```

**Expected Results:**
- Calendar: ~36,890 records
- Store Data: ~1,600 records
- Employee Data: ~31,000 records
- Menu Data: ~1,000 records
- Inventory: ~2,000 records
- Promotions: ~250 records
- Loyalty: ~2,000 records
- **Orders: 5,000,000+ records** ⚠️
- Finance: ~20,000 records
- System: ~10 records

---

### **3. Check for Nulls in Critical Columns**

```python
# Calendar - CalendarID should never be null
cal_df = spark.read.parquet("/mnt/restaurant_input/dim")
null_count = cal_df.filter("CalendarID IS NULL").count()
print(f"Calendar null CalendarIDs: {null_count} (should be 0)")

# Orders - OrderID, TotalAmount should never be null
ord_df = spark.read.parquet("/mnt/restaurant_input/ord")
null_orders = ord_df.filter("OrderID IS NULL").count()
null_amounts = ord_df.filter("TotalAmount IS NULL").count()
print(f"Orders null OrderIDs: {null_orders} (should be 0)")
print(f"Orders null TotalAmounts: {null_amounts} (should be 0)")

# Employees - EmployeeID should never be null
emp_df = spark.read.parquet("/mnt/restaurant_input/emp")
null_emps = emp_df.filter("EmployeeID IS NULL").count()
print(f"Employees null EmployeeIDs: {null_emps} (should be 0)")
```

---

### **4. Check for Duplicate Primary Keys**

```python
# Check Orders for duplicates
ord_df = spark.read.parquet("/mnt/restaurant_input/ord").filter("OrderID IS NOT NULL")
total_count = ord_df.count()
distinct_count = ord_df.select("OrderID").distinct().count()
duplicates = total_count - distinct_count
print(f"Orders: {total_count:,} total, {distinct_count:,} distinct")
print(f"Duplicates: {duplicates} (should be 0)")

# Check Calendar for duplicates
cal_df = spark.read.parquet("/mnt/restaurant_input/dim")
cal_total = cal_df.count()
cal_distinct = cal_df.select("CalendarID").distinct().count()
cal_dupes = cal_total - cal_distinct
print(f"Calendar: {cal_total:,} total, {cal_distinct:,} distinct")
print(f"Duplicates: {cal_dupes} (should be 0)")

# Check States for duplicates
store_df = spark.read.parquet("/mnt/restaurant_input/store")
states_df = store_df.filter("StateID IS NOT NULL")
states_total = states_df.count()
states_distinct = states_df.select("StateID").distinct().count()
states_dupes = states_total - states_distinct
print(f"States: {states_total} total, {states_distinct} distinct")
print(f"Duplicates: {states_dupes} (should be 0)")
```

---

### **5. Validate Business Rules**

```python
from pyspark.sql.functions import col

# Rule 1: Orders - TotalAmount should >= SubtotalAmount
ord_df = spark.read.parquet("/mnt/restaurant_input/ord")
invalid_totals = ord_df.filter(col("TotalAmount") < col("SubtotalAmount")).count()
print(f"Orders with TotalAmount < SubtotalAmount: {invalid_totals} (should be 0)")

# Rule 2: Orders - TaxAmount should be positive
invalid_tax = ord_df.filter((col("TaxAmount") < 0) | (col("TaxAmount").isNull())).count()
print(f"Orders with invalid TaxAmount: {invalid_tax} (should be 0)")

# Rule 3: Menu - BasePrice should be > 0
menu_df = spark.read.parquet("/mnt/restaurant_input/menu")
invalid_prices = menu_df.filter((col("BasePrice") <= 0) | (col("BasePrice").isNull())).count()
print(f"Menu items with invalid BasePrice: {invalid_prices} (should be 0)")

# Rule 4: Calendar - IsWeekend should match DayOfWeek
cal_df = spark.read.parquet("/mnt/restaurant_input/dim")
invalid_weekend = cal_df.filter(
    ((col("DayOfWeek").isin([6, 7]) & (col("IsWeekend") != 1)) |
     (~col("DayOfWeek").isin([6, 7]) & (col("IsWeekend") == 1)))
).count()
print(f"Calendar records with mismatched IsWeekend: {invalid_weekend} (should be 0)")

# Rule 5: Employee hourly rates should be reasonable
emp_df = spark.read.parquet("/mnt/restaurant_input/emp")
invalid_rates = emp_df.filter(
    (col("HourlyRate") < 10) | (col("HourlyRate") > 100) | (col("HourlyRate").isNull())
).count()
print(f"Employees with invalid HourlyRate: {invalid_rates} (should be 0)")
```

---

### **6. Validate Foreign Key Relationships**

```python
# FK 1: Orders.OrderDateID -> Calendar.CalendarID
cal_df = spark.read.parquet("/mnt/restaurant_input/dim")
ord_df = spark.read.parquet("/mnt/restaurant_input/ord")

order_dates = ord_df.select("OrderDateID").distinct()
calendar_ids = cal_df.select("CalendarID").distinct()

orphaned_dates = order_dates.join(
    calendar_ids, 
    order_dates.OrderDateID == calendar_ids.CalendarID, 
    "left_anti"
).count()
print(f"Orders with invalid OrderDateID: {orphaned_dates} (should be 0)")

# FK 2: Orders.LocationID -> Locations.LocationID
store_df = spark.read.parquet("/mnt/restaurant_input/store")
locations = store_df.filter("LocationID IS NOT NULL").select("LocationID").distinct()

order_locations = ord_df.select("LocationID").distinct()
orphaned_locations = order_locations.join(
    locations,
    order_locations.LocationID == locations.LocationID,
    "left_anti"
).count()
print(f"Orders with invalid LocationID: {orphaned_locations} (should be 0)")

# FK 3: Employees.PositionID -> Positions.PositionID
emp_df = spark.read.parquet("/mnt/restaurant_input/emp")
positions = emp_df.filter("PositionID IS NOT NULL AND PositionCode IS NOT NULL").select("PositionID").distinct()
employees = emp_df.filter("EmployeeID IS NOT NULL").select("PositionID").distinct()

orphaned_positions = employees.join(
    positions,
    employees.PositionID == positions.PositionID,
    "left_anti"
).count()
print(f"Employees with invalid PositionID: {orphaned_positions} (should be 0)")
```

---

### **7. Check Data Distribution**

```python
from pyspark.sql.functions import year, month, countDistinct

# Orders by year
ord_df = spark.read.parquet("/mnt/restaurant_input/ord")
print("\nOrders by Year:")
ord_df.withColumn("Year", year("OrderDate")) \
    .groupBy("Year") \
    .count() \
    .orderBy("Year") \
    .show()

# Orders by location (top 10)
print("\nTop 10 Locations by Order Count:")
ord_df.groupBy("LocationID") \
    .count() \
    .orderBy("count", ascending=False) \
    .limit(10) \
    .show()

# Orders by status
print("\nOrders by Status:")
ord_df.groupBy("OrderStatus") \
    .count() \
    .show()

# Average order value
print("\nAverage Order Value:")
ord_df.select(avg("TotalAmount").alias("AvgOrderValue")).show()
```

---

### **8. Verify Data Completeness**

```python
# Check if all expected states are present
store_df = spark.read.parquet("/mnt/restaurant_input/store")
states_df = store_df.filter("StateID IS NOT NULL")

expected_states = 51  # All US states + DC
actual_states = states_df.select("StateID").distinct().count()

print(f"States: {actual_states} / {expected_states} expected")
if actual_states != expected_states:
    print("⚠️  WARNING: Not all states are present!")
    
# List all states
print("\nAll States:")
states_df.select("StateCode", "StateName").distinct().orderBy("StateCode").show(60, False)

# Check if all locations have orders
locations_df = store_df.filter("LocationID IS NOT NULL")
ord_df = spark.read.parquet("/mnt/restaurant_input/ord")

total_locations = locations_df.select("LocationID").distinct().count()
locations_with_orders = ord_df.select("LocationID").distinct().count()

coverage = (locations_with_orders / total_locations) * 100
print(f"\nLocation Coverage: {locations_with_orders}/{total_locations} ({coverage:.1f}%)")
if coverage < 80:
    print("⚠️  WARNING: Less than 80% of locations have orders!")
```

---

### **9. Performance Metrics**

```python
from pyspark.sql.functions import sum as _sum, avg, min as _min, max as _max

ord_df = spark.read.parquet("/mnt/restaurant_input/ord")

# Calculate key metrics
metrics = ord_df.select(
    _sum("TotalAmount").alias("TotalRevenue"),
    avg("TotalAmount").alias("AvgOrderValue"),
    _min("TotalAmount").alias("MinOrderValue"),
    _max("TotalAmount").alias("MaxOrderValue"),
    countDistinct("LocationID").alias("UniqueLocations"),
    countDistinct("MemberID").alias("UniqueMembers")
).collect()[0]

print("\n" + "=" * 60)
print("BUSINESS METRICS")
print("=" * 60)
print(f"Total Revenue: ${metrics['TotalRevenue']:,.2f}")
print(f"Average Order Value: ${metrics['AvgOrderValue']:.2f}")
print(f"Min Order Value: ${metrics['MinOrderValue']:.2f}")
print(f"Max Order Value: ${metrics['MaxOrderValue']:.2f}")
print(f"Unique Locations: {metrics['UniqueLocations']:,}")
print(f"Unique Members: {metrics['UniqueMembers']:,}")
print("=" * 60)
```

---

## 🎯 Expected Validation Results

### **All Checks Should Pass ✅**

If validation is successful, you should see:

```
✅ All 10 folders exist
✅ All row counts within expected ranges
✅ No null values in required columns
✅ No duplicate primary keys
✅ All foreign keys valid
✅ All business rules satisfied
✅ Data distribution looks normal
```

### **Common Issues and Solutions**

| Issue | Likely Cause | Solution |
|-------|-------------|----------|
| Folder not found | Pipeline step didn't run | Re-run that specific file |
| Low row count | Pipeline failed partially | Check logs, re-run failed step |
| Duplicate keys | Multiple pipeline runs | Clear folder and re-run |
| Invalid FKs | Dependencies not satisfied | Run files in correct order |
| Null values | Data generation bug | Report issue, check source file |

---

## 📊 Validation Checklist

Use this checklist after running validations:

### **Critical Checks** (Must Pass)
- [ ] All 10 folders exist
- [ ] Calendar has ~36,890 records
- [ ] 51 states present
- [ ] At least 100 locations
- [ ] At least 500 employees
- [ ] At least 1 million orders
- [ ] No null primary keys
- [ ] No duplicate primary keys
- [ ] All foreign keys valid

### **Important Checks** (Should Pass)
- [ ] Orders span multiple dates
- [ ] Orders from multiple locations
- [ ] TotalAmount >= SubtotalAmount
- [ ] All prices > 0
- [ ] Tax amounts reasonable (0-15% of subtotal)
- [ ] 80%+ location coverage in orders

### **Optional Checks** (Nice to Have)
- [ ] Order items average 2-3 per order
- [ ] Finance aggregations match order totals
- [ ] Employee schedules cover all employees
- [ ] Menu items distributed across categories

---

## 🚀 Next Steps After Validation

### **If All Validations Pass ✅**

1. **Create Delta tables for better performance:**
   ```python
   # Convert to Delta format
   spark.read.parquet("/mnt/restaurant_input/ord") \
       .write.format("delta").mode("overwrite") \
       .save("/mnt/restaurant_delta/ord")
   ```

2. **Set up analytics dashboards**
3. **Schedule incremental updates**
4. **Document your data model**

### **If Some Validations Fail ❌**

1. **Review the specific error messages**
2. **Check the failed table's generation file**
3. **Re-run that specific step:**
   ```python
   %run ./0_calendar.py  # Or whichever file failed
   ```
4. **Re-run validation to confirm fix**

---

## 📞 Support

- **Quick issues**: Use `validation_quick.py`
- **Detailed analysis**: Use `validation_comprehensive.py`
- **Custom checks**: Use the manual commands above
- **Debugging**: Check individual table files

---

**Ready to validate?** Start with the quick check:

```python
%run ./validation_quick.py
```

Good luck! 🚀
