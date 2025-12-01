# =========================================================
# 1. dbo.calendar TABLE INSERT
# =========================================================
calendar_df = spark.createDataFrame([
    (1, "2024-01-01", 2024, "Q1", 1, "2023-12-31", "2024-01-06", "2024-01-01", "2024-01-31"),
    (2, "2024-01-02", 2024, "Q1", 1, "2023-12-31", "2024-01-06", "2024-01-01", "2024-01-31")
], [
    "calendar_key","financial_date","financial_year","quarter",
    "week_number","week_start_date","week_end_date",
    "financial_month_start_date","financial_month_end_date"
])
# =========================================================
# 2. dbo.promotions TABLE INSERT
# =========================================================
promotions_df = spark.createDataFrame([
    (1, "New Year Offer", 10.5),
    (2, "Weekend Special", 5.0)
], ["promotion_id","promotion","promotion_value"])

# =========================================================
# 3. emp.department TABLE INSERT
# =========================================================
department_df = spark.createDataFrame([
    (1, "Kitchen"),
    (2, "Service"),
    (3, "Cleaning")
], ["department_id","department"])

# =========================================================
# 4. emp.employee TABLE INSERT
# =========================================================
employee_df = spark.createDataFrame([
    (1, "Sai", "Y", 1),
    (2, "John", "Y", 2),
    (3, "Ravi", "N", 1)
], ["employee_id","emp_name","emp_status","department_id"])

# =========================================================
# 5. emp.salary TABLE INSERT
# =========================================================
salary_df = spark.createDataFrame([
    (1, 15.5, "2024-01-01 09:00:00", "2024-01-01 17:00:00"),
    (2, 18.0, "2024-01-02 10:00:00", "2024-01-02 18:00:00")
], ["salary_id","pay_rate","emp_start_time","emp_end_time"])

# =========================================================
# 6. menu.menu_group TABLE INSERT
# =========================================================
menu_group_df = spark.createDataFrame([
    (1, "Starters"),
    (2, "Main Course"),
    (3, "Desserts")
], ["menu_group_id","menu_group"])

# =========================================================
# 7. menu.menu_item TABLE INSERT
# =========================================================
menu_item_df = spark.createDataFrame([
    (1, "Chicken Wings", 12.5, 1),
    (2, "Paneer Tikka", 10.0, 1),
    (3, "Burger", 8.5, 2),
    (4, "Chocolate Cake", 6.0, 3)
], ["menu_item_id","item","amount","menu_group_id"])

# =========================================================
# 8. sh.sales TABLE INSERT (ARRAY COLUMN)
# =========================================================
sales_df = spark.createDataFrame([
    (1, 1, [1, 2], 22.5, "CARD", 101),
    (2, 2, [3], 8.5, "CASH", 102),
    (3, 1, [4, 3], 14.5, "UPI", 103)
], ["sale_id","employee_id","menu_item_id","total_amount","payment_mode","customer_id"])


