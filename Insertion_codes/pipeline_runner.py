"""
Spicy Fiesta Data Generation Pipeline - Databricks Compatible
Executes all data generation files in sequence using shared Spark session
"""

from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

print("=" * 80)
print("SPICY FIESTA DATA GENERATION PIPELINE")
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

# Define all scripts in execution order
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
    ("12/14", "Orders (may take time)", "11_ord_generate_orders.py"),
    ("13/14", "Finance daily sales/expenses", "12_finance_daily_sales_store_expenses.py"),
    ("14/14", "System configuration/logs", "13_dbo_audit_system_error.py"),
]

# Track results
results = {"success": [], "failed": []}

# Execute each script in sequence
for step, description, script_file in scripts:
    print(f"\n{'='*80}")
    print(f"[{step}] {description}")
    print(f"Script: {script_file}")
    print(f"{'='*80}")
    
    try:
        # Read the Python file
        with open(script_file, 'r', encoding='utf-8') as f:
            script_code = f.read()
        
        # Create execution namespace with Databricks shared resources
        exec_namespace = {
            '__name__': '__main__',
            '__file__': script_file,
            'spark': spark,          # Databricks shared Spark session
            'sc': sc,                # Databricks shared Spark context  
            'dbutils': dbutils,      # Databricks utilities
            'sqlContext': sqlContext # SQL context
        }
        
        # Execute the script in the namespace
        exec(script_code, exec_namespace)
        
        # Track success
        results["success"].append((step, script_file))
        print(f"✅ [{step}] {script_file} completed successfully")
        
    except Exception as e:
        # Track failure
        results["failed"].append((step, script_file, str(e)))
        print(f"❌ [{step}] {script_file} FAILED")
        print(f"Error: {str(e)}")
        logger.error(f"Failed on {script_file}: {str(e)}", exc_info=True)
        
        # Stop on first failure (remove these lines to continue despite errors)
        print("\n⚠️  Stopping pipeline due to failure")
        break

# Print summary
print("\n" + "=" * 80)
print("EXECUTION SUMMARY")
print("=" * 80)
print(f"Total scripts: {len(scripts)}")
print(f"✅ Successful: {len(results['success'])}")
print(f"❌ Failed: {len(results['failed'])}")
print()

if results['success']:
    print("✅ Completed scripts:")
    for step, script in results['success']:
        print(f"  [{step}] {script}")

if results['failed']:
    print("\n❌ Failed scripts:")
    for step, script, error in results['failed']:
        print(f"  [{step}] {script}")
        print(f"       Error: {error[:200]}")
else:
    print("\n🎉 ALL SCRIPTS COMPLETED SUCCESSFULLY!")

print(f"\nFinished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)
