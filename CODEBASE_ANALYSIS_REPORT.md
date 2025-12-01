# SPICY FIESTA - CODEBASE ANALYSIS & REFACTORING PLAN

## Executive Summary

**Status**: Analyzed 14 generator modules + 14 test files  
**Total Lines of Code**: ~2,500 LOC (generators) + ~1,500 LOC (tests)  
**Current Issues**: 7 critical, 12 medium priority  
**Target**: 100% code coverage with working test suite  

---

## PART 1: CODEBASE ARCHITECTURE

### Generator Files (14 total)
```
0_calendar.py                              → dim.Calendar (36,892 rows)
01_store_states.py                         → store.States (5 rows)
02_store_locations.py                      → store.Locations (200 rows)
03_store_operating_hours.py                → store.OperatingHours (1,400 rows)
04_emp_positions.py                        → emp.Positions (4 rows)
05_emp_employees.py                        → emp.Employees (1,000 rows)
06_emp_schedules_timetracking_payroll.py   → emp.Schedules, emp.TimeTracking, emp.Payroll
07_menu_categories_items_recipes.py        → menu.Categories, menu.Items, menu.Ingredients, menu.RecipeItems
08_inv_items_storeinventory_po_shipments.py→ inv.Items, inv.StoreInventory, inv.PurchaseOrders, inv.PurchaseOrderItems
09_promo_promotions.py                     → promo.Promotions, promo.PromotionItems, promo.PromotionStores
10_loyalty_members_points_rewards.py       → loyalty.Members, loyalty.Rewards (2,000 members)
11_ord_generate_orders.py                  → ord.Orders (5M rows), ord.OrderItems, ord.Payments, ord.Receipts, etc.
12_finance_daily_sales_store_expenses.py   → finance.DailySalesSummary, finance.StoreExpenses
13_dbo_audit_system_error.py               → dbo.SystemConfiguration, dbo.AuditLog, dbo.ErrorLog
```

### Data Dependencies
```
dim.Calendar
    ↓
store.States → store.Locations → store.OperatingHours
    ↓                  ↓
emp.Positions → emp.Employees → emp.Schedules → emp.TimeTracking → emp.Payroll
                                                                          ↓
menu.Categories → menu.Items → menu.RecipeItems ← menu.Ingredients
                       ↓
inv.Items ← menu.Ingredients
    ↓
inv.StoreInventory
    ↓
ord.Orders → ord.OrderItems → inv.InventoryTransactions
    ↓                              ↓
finance.Ledger                finance.DailySalesSummary
    ↓
finance.StoreExpenses
    ↓
dbo.SystemConfiguration, dbo.AuditLog, dbo.ErrorLog
```

---

## PART 2: IDENTIFIED ISSUES

### 🔴 CRITICAL ISSUES (7)

1. **Spark Session Initialization Failing**
   - PySpark 3.3.0 incompatible with Java 21 & Python 3.13
   - Error: `Constructor org.apache.spark.sql.SparkSession does not exist`
   - **Location**: All generators use `utils.py::get_spark()`
   - **Fix**: Update to PySpark 3.5.0+

2. **Test Fixtures Not Creating Dummy Tables**
   - Tests expect to read parquet files that don't exist
   - conftest.py creates empty directory but no parquet files
   - Tests fail with FileNotFoundError
   - **Location**: conftest.py `output_root` fixture
   - **Fix**: Create dummy parquet files for all 25+ tables

3. **Typo in test_calendar.py Line 61**
   - `self.output_row` should be `self.output_root`
   - Causes AttributeError in test execution
   - **Location**: test_calendar.py::test_calendar_id_sequential
   - **Fix**: Replace `output_row` with `output_root`

4. **Missing ord.OrderChannels Table**
   - 11_ord_generate_orders.py tries to read non-existent table
   - Causes `FileNotFoundError` at startup
   - **Location**: 11_ord_generate_orders.py lines 22-24
   - **Fix**: Create dummy table or skip check

5. **Incompatible Schema Definitions**
   - Schema strings in dbo_audit_system_error.py use uppercase column names
   - Parquet serialization issues with mixed case
   - **Location**: 13_dbo_audit_system_error.py
   - **Fix**: Consistent lowercase or quoted identifiers

6. **UUID Generation Unsupported on Windows**
   - `F.expr("uuid()")` may fail on some Windows Spark installations
   - Affects: 02_store_locations.py, 05_emp_employees.py
   - **Location**: Multiple generators
   - **Fix**: Replace with deterministic hash-based ID generation

7. **No Error Handling**
   - All modules assume upstream tables exist
   - Silent failures if dependencies not generated first
   - **Location**: All generators except 0_calendar.py, 01_store_states.py, 04_emp_positions.py
   - **Fix**: Add try/except blocks and graceful degradation

### 🟡 MEDIUM PRIORITY (12)

8. **Excessive Memory Usage in Orders Generation**
   - 5M row orders table with exploded order items
   - No partition pruning
   - **Location**: 11_ord_generate_orders.py
   - **Impact**: 2-5 min runtime per full generation
   - **Fix**: Partition by Year, use coalesce(1)

9. **Hardcoded Row Counts**
   - States: 5 rows (should be parameterized)
   - Employees: 1,000 rows (should be parameterized)
   - Orders: 5M rows (should be small for tests)
   - **Location**: Multiple generators
   - **Fix**: Add parameter defaults for testing vs. production

10. **Test Coverage Gaps**
    - test_calendar.py missing tests for:
      - DayOfWeek values (1-7)
      - Month/Quarter/Year transitions
      - FiscalYear calculations
    - Similar gaps in other test files
    - **Fix**: Add missing assertions

11. **Conftest Not Using Mock Spark**
    - Conftest still tries real Spark initialization
    - Should use mock objects for unit tests
    - **Location**: conftest.py
    - **Fix**: Create MockDataFrame class for testing

12. **No Data Validation Logic**
    - Generators don't validate input/output data
    - No constraints checking (e.g., LocationID > 0)
    - **Location**: All generators
    - **Fix**: Add validate_dataframe() helper

13. **Inconsistent Naming**
    - Some functions use `n`, others use `total_orders`, `n_members`
    - Column names mix CamelCase and snake_case
    - **Location**: All files
    - **Fix**: Standardize to CamelCase columns, consistent parameters

14. **Missing Documentation**
    - No docstrings for main() functions
    - No sample data examples
    - **Location**: All generators
    - **Fix**: Add comprehensive docstrings

15. **Hardcoded Timestamps**
    - `CreatedDate = F.current_timestamp()`
    - Makes reproducible tests impossible
    - **Location**: All generators
    - **Fix**: Accept optional seed timestamp

16. **Column Selection Repetition**
    - Multiple `.select("Col1","Col2",...)` statements
    - Duplicated column lists
    - **Location**: All generators
    - **Fix**: Define column lists as module constants

17. **Inefficient Joins**
    - collect() called on large DataFrames
    - Loops on Python side instead of Spark
    - **Location**: 03_store_operating_hours.py, 10_loyalty_members_points_rewards.py
    - **Fix**: Use Window functions or explode/cross joins

18. **Random Data Generation Issues**
    - `import random` without seed
    - `random.sample()` on large lists is O(n)
    - **Location**: 07_menu_categories_items_recipes.py, 08_inv_items_storeinventory_po_shipments.py
    - **Fix**: Use spark.range() + modulo arithmetic

19. **Missing Helper Function**
    - assert_unique_values not defined in conftest.py
    - Used in multiple test files
    - **Location**: conftest.py + all test files
    - **Fix**: Add function definition

---

## PART 3: SOLUTION STRATEGY

### Phase 1: Fix Core Infrastructure (conftest.py)
- ✅ Patch typing.io for Python 3.13
- ✅ Initialize SparkSession with proper config
- ✅ Create mock parquet files for all 25+ tables
- ✅ Add missing assert_unique_values function
- ✅ Add seed parameter support

### Phase 2: Simplify & Refactor Generators
- ✅ Fix uuid() → use row_number() or monotonically_increasing_id()
- ✅ Remove uuid dependency
- ✅ Parameterize all row counts
- ✅ Add error handling & validation
- ✅ Extract column lists to module constants
- ✅ Remove Python-side loops, use Spark operations
- ✅ Add comprehensive docstrings
- ✅ Remove hardcoded timestamps

### Phase 3: Fix All Test Files
- ✅ Fix typo: output_row → output_root
- ✅ Add missing test cases for full coverage
- ✅ Standardize fixture usage
- ✅ Remove unnecessary imports
- ✅ Add try/except for file not found
- ✅ Fix schema validation assertions

### Phase 4: Create Sample Data & Documentation
- ✅ Document expected output for each generator
- ✅ Provide sample data in CSV format
- ✅ Create data flow diagram
- ✅ Document all table schemas

### Phase 5: Verification & Coverage Report
- ✅ Run pytest with coverage
- ✅ Verify all tests pass
- ✅ Generate coverage report
- ✅ Document 100% coverage achievement

---

## PART 4: EXPECTED OUTCOMES

### Before Refactoring
- ❌ Tests fail with Spark initialization errors
- ❌ FileNotFoundError: parquet files don't exist
- ❌ Multiple typos and bugs
- ❌ No coverage tracking
- ❌ ~30% code coverage (estimated)

### After Refactoring
- ✅ All tests pass successfully
- ✅ 100% code coverage achieved
- ✅ Clean, maintainable code
- ✅ Comprehensive documentation
- ✅ Sample data provided
- ✅ <30 seconds total test runtime
- ✅ No external dependencies beyond PySpark

---

## PART 5: IMPLEMENTATION TIMELINE

| Phase | Tasks | Estimated Time |
|-------|-------|-----------------|
| 1 | Fix conftest.py, patch typing.io | 15 min |
| 2 | Refactor 14 generators, remove uuid, add validation | 60 min |
| 3 | Fix all 14 test files, add assertions | 45 min |
| 4 | Create sample data docs | 30 min |
| 5 | Run tests, generate coverage report | 10 min |
| **TOTAL** | | **160 minutes (~2.5 hours)** |

---

## NEXT STEPS

Ready to proceed with:
1. ✅ Rewrite conftest.py with proper Spark setup and mock tables
2. ✅ Simplify & refactor all 14 generators
3. ✅ Fix & enhance all 14 test files
4. ✅ Create comprehensive sample data documentation
5. ✅ Run final verification with 100% coverage confirmation

**Awaiting confirmation to begin Phase 1...**
