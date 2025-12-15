"""
ONE-CLICK FIX AND RUN
Automatically fixes all files and runs the complete pipeline
Upload this to Databricks and run: %run ./one_click_fix_and_run.py
"""

import re
import os
import shutil
from datetime import datetime

print("=" * 80)
print("SPICY FIESTA PIPELINE - ONE-CLICK FIX & RUN")
print("=" * 80)
print()

# ============================================================================
# STEP 1: BACKUP
# ============================================================================
print("[STEP 1/4] Creating backup...")

files_to_backup = [
    "0_calendar.py", "01_store_states.py", "02_store_locations.py",
    "03_store_operating_hours.py", "04_emp_positions.py", "05_emp_employees.py",
    "06_emp_schedules_timetracking_payroll.py", "07_menu_categories_items_recipes.py",
    "08_inv_items_storeinventory_po_shipments.py", "09_promo_promotions.py",
    "10_loyalty_members_points_rewards.py", "11_ord_generate_orders.py",
    "12_finance_daily_sales_store_expenses.py", "13_dbo_audit_system_error.py",
    "utils.py"
]

backup_dir = "backup_" + datetime.now().strftime("%Y%m%d_%H%M%S")
os.makedirs(backup_dir, exist_ok=True)

for file in files_to_backup:
    if os.path.exists(file):
        shutil.copy2(file, f"{backup_dir}/{file}")

print(f"✅ Backup created: {backup_dir}")
print()

# ============================================================================
# STEP 2: FIX FILES (Remove spark.stop())
# ============================================================================
print("[STEP 2/4] Removing spark.stop() from all files...")

def remove_spark_stop(content):
    """Remove all spark.stop() calls."""
    patterns = [
        r'\s*spark\.stop\(\)\s*\n',
        r'\s*spark\.stop\(\)',
        r'finally:\s*spark\.stop\(\)',
    ]
    for pattern in patterns:
        content = re.sub(pattern, '', content)
    content = re.sub(r'finally:\s*$', '', content, flags=re.MULTILINE)
    return content

fixed_count = 0
for filename in files_to_backup[:-1]:  # Exclude utils.py
    if os.path.exists(filename):
        with open(filename, 'r') as f:
            content = f.read()
        
        if 'spark.stop()' in content:
            fixed_content = remove_spark_stop(content)
            with open(filename, 'w') as f:
                f.write(fixed_content)
            fixed_count += 1
            print(f"  ✅ Fixed: {filename}")
        else:
            print(f"  ✓ Clean: {filename}")

print(f"\n✅ Fixed {fixed_count} files")
print()

# ============================================================================
# STEP 3: UPDATE UTILS.PY
# ============================================================================
print("[STEP 3/4] Updating utils.py...")

new_utils_content = '''"""
Utility functions for data insertion pipeline - Databricks Compatible.
"""
import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_spark(app_name: str, master: str = None) -> SparkSession:
    """Get the shared Spark session in Databricks."""
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info(f"Using shared Databricks Spark session for: {app_name}")
        return spark
    except Exception as e:
        logger.error(f"Failed to get Spark session: {e}")
        raise


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    partitionBy: Optional[str] = None
) -> None:
    """Write DataFrame to Parquet format."""
    if df is None:
        raise ValueError("DataFrame cannot be None")
    
    try:
        row_count = df.count()
        if row_count == 0:
            logger.warning(f"Writing empty DataFrame to {path}")
        
        writer = df.write.mode(mode)
        
        if partitionBy:
            if isinstance(partitionBy, str):
                writer = writer.partitionBy(partitionBy)
            elif isinstance(partitionBy, list):
                writer = writer.partitionBy(*partitionBy)
        
        writer.parquet(path)
        logger.info(f"Successfully written {row_count} rows to {path}")
        
    except Exception as e:
        logger.error(f"Failed to write Parquet to {path}: {e}")
        raise


def validate_output_path(output_path: str) -> None:
    """Validate output path."""
    if not output_path or not isinstance(output_path, str):
        raise ValueError("output_root must be a non-empty string")


def check_dataframe_schema(df: DataFrame, expected_columns: list) -> bool:
    """Validate DataFrame schema."""
    actual_columns = set(df.columns)
    expected_set = set(expected_columns)
    
    missing = expected_set - actual_columns
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    
    logger.info(f"Schema validation passed: all {len(expected_columns)} columns present")
    return True
'''

# Backup original utils.py
if os.path.exists("utils.py"):
    shutil.copy2("utils.py", f"{backup_dir}/utils_original.py")

# Write new utils.py
with open("utils.py", 'w') as f:
    f.write(new_utils_content)

print("✅ utils.py updated for Databricks")
print()

# ============================================================================
# STEP 4: RUN PIPELINE
# ============================================================================
print("[STEP 4/4] Running pipeline...")
print("=" * 80)
print()

scripts = [
    ("1/14", "Calendar dimension", "0_calendar.py"),
    ("2/14", "Store states", "01_store_states.py"),
    ("3/14", "Store locations", "02_store_locations.py"),
    ("4/14", "Store operating hours", "03_store_operating_hours.py"),
    ("5/14", "Employee positions", "04_emp_positions.py"),
    ("6/14", "Employees", "05_emp_employees.py"),
    ("7/14", "Employee schedules/time tracking/payroll", "06_emp_schedules_timetracking_payroll.py"),
    ("8/14", "Menu categories/items/recipes", "07_menu_categories_items_recipes.py"),
    ("9/14", "Inventory items/purchase orders", "08_inv_items_storeinventory_po_shipments.py"),
    ("10/14", "Promotions", "09_promo_promotions.py"),
    ("11/14", "Loyalty members/rewards", "10_loyalty_members_points_rewards.py"),
    ("12/14", "Orders", "11_ord_generate_orders.py"),
    ("13/14", "Finance daily sales/expenses", "12_finance_daily_sales_store_expenses.py"),
    ("14/14", "System configuration/logs", "13_dbo_audit_system_error.py"),
]

results = {"success": [], "failed": []}

for step, description, script_file in scripts:
    print(f"\n{'='*80}")
    print(f"[{step}] {description}")
    print(f"{'='*80}")
    
    try:
        with open(script_file, 'r') as f:
            script_code = f.read()
        
        exec_namespace = {
            '__name__': '__main__',
            '__file__': script_file,
            'spark': spark,
            'sc': sc,
            'dbutils': dbutils,
            'sqlContext': sqlContext
        }
        
        exec(script_code, exec_namespace)
        
        results["success"].append((step, script_file))
        print(f"✅ Completed: {script_file}")
        
    except Exception as e:
        results["failed"].append((step, script_file, str(e)))
        print(f"❌ FAILED: {script_file}")
        print(f"Error: {str(e)}")
        print("\n⚠️  Stopping pipeline due to failure")
        break

# Summary
print("\n" + "=" * 80)
print("FINAL SUMMARY")
print("=" * 80)
print(f"Backup location: {backup_dir}")
print(f"Files fixed: {fixed_count}")
print(f"Scripts successful: {len(results['success'])}/{len(scripts)}")
print(f"Scripts failed: {len(results['failed'])}")

if results['failed']:
    print("\n❌ Failed scripts:")
    for step, script, error in results['failed']:
        print(f"  [{step}] {script}: {error[:100]}")
else:
    print("\n🎉 ALL SCRIPTS COMPLETED SUCCESSFULLY!")

print("\n" + "=" * 80)
