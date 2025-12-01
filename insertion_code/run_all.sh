#!/usr/bin/env bash
OUT=${1:-./output_parquet}

# 1 - Calendar
spark-submit 00_calendar.py $OUT
# 2 - States, locations, operating hours
spark-submit 01_store_states.py $OUT
spark-submit 02_store_locations.py $OUT 200
spark-submit 03_store_operating_hours.py $OUT
# 3 - Positions & Employees & schedules/time tracking
spark-submit 04_emp_positions.py $OUT
spark-submit 05_emp_employees.py $OUT 1000
spark-submit 06_emp_schedules_timetracking_payroll.py $OUT
# 4 - Menu & recipes
spark-submit 07_menu_categories_items_recipes.py $OUT
# 5 - Inventory and POs
spark-submit 08_inv_items_storeinventory_po_shipments.py $OUT
# 6 - Promos and loyalty
spark-submit 09_promo_promotions.py $OUT
spark-submit 10_loyalty_members_points_rewards.py $OUT 2000
# 7 - Orders + items + payments + inventory consumption + ledger
spark-submit 11_ord_generate_orders.py $OUT 5000000
# 8 - Finance aggregates
spark-submit 12_finance_daily_sales_store_expenses.py $OUT
# 9 - DBO tables
spark-submit 13_dbo_audit_system_error.py $OUT
