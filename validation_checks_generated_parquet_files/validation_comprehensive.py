"""
Comprehensive Validation Suite for Spicy Fiesta Data Pipeline
Run this after pipeline execution to verify data quality and completeness
"""

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

# Validation results tracker
validation_results = {
    "passed": [],
    "failed": [],
    "warnings": []
}

def df_creator(path):
    source_df = spark.read.option("recursiveFileLookup","true").parquet(path)
    return source_df

def null_count_checker(df):
    total_rows = df.count()
    if total_rows:
        null_df = df.select(*
                                     [
                                         (((sum(when(col(c).isnull(),1))) / total_rows)*100).alias(f"{c}_null_percentage") for c in calendar_df.columns
                                     ])
        columns_null_df = null_df.filter(null_df[f"{c}_null_percentage"] > 30 for c in calendar_df.columns)
    else:
        validation_results["failed"].append("No rows in the parquet file")
    return columns_null_df

def calendar_validation_check():
    print("Validation check for the calendar table")
    print("=="*100)
    CALENDAR_PATH = f"{MOUNT_POINT}/01"
    columns = [
        "CalendarID", "CalendarDate", "Year", "Quarter", "Month", "MonthName", "Day",
        "DayOfWeek", "DayName", "WeekOfYear", "ISOYear", "ISOWeek", "IsWeekend",
        "IsHoliday", "HolidayName", "IsMonthStart", "IsMonthEnd", "IsQuarterStart",
        "IsQuarterEnd", "IsYearStart", "IsYearEnd", "FiscalYear", "FiscalMonth",
        "FiscalQuarter", "DayOfYear", "CreatedDate"
    ]
    print("=="*10)
    print("Checking the column exisitence in the parquet file data")
    print("=="*10)
    calendar_df = df_creator(CALENDAR_PATH)
    if calandar_df.columns == columns:
        logger.info("All columns are present in the calendar table.")
        validation_results["passed"].append("All columns are present in the calendar parquet file.")
    else:
        logger.warning("columns are missing in the output file.")
    year_check_df = calandar_df.select("Year").distinct()
    for i in range(1980,2081):
        year_list = year_check_df.collect()
        if i in year_list:
            logger.info(f"Year {i} is present in the calendar table.")
        else:
            logger.warning(f"Year {i} is missing in the calendar table.")
            validation_results["failed"].append(f"Year {i} is missing in the calendar parquet.")
    print("=="*10)
    print("Checking the null percentage in the parquet file data")
    print("=="*10)
    null_percentage_df = null_count_checker(calendar_df)
    if null_percentage_df.count() == 0:
        logger.info("No columns have null percentage more than 30%.")
        validation_results["passed"].append("No columns have null percentage more than 30%.")
    else:
        logger.warning("Columns have null percentage more than 30%.")
        validation_results["failed"].append("Columns have null percentage more than 30%.")
    print("=="*10)
    print("Checking the data type of the columns in the parquet file data")
    print("=="*10)
    validation_check_list = {
        "CalendarID":"integer"
        "CalendarDate":"date"
        "Quarter":"integer"
        "Month":"integer"
        "MonthName":"string"
        "Day":"integer"
        "DayOfWeek":"integer"
        "DayName":"string"
        "WeekOfYear":"integer"
        "ISOYear":"integer"
        "ISOWeek":"integer"
        "IsWeekend":"integer"
        "IsHoliday":"integer"
        "HolidayName":"string"
        "IsMonthStart":"integer"
        "IsMonthEnd":"integer"
        "IsQuarterStart":"integer"
        "IsQuarterEnd":"integer"
        "IsYearStart":"integer"
        "IsYearEnd":'integer'
        'FiscalYear':'integer'
        'FiscalMonth':'integer'
        'FiscalQuarter':'integer'
        'DayOfYear':'integer'
        'CreatedDate':'timestamp'
    }
    for key,values in validation_check_list.items():
        if calendar_df.schema[key].dataType.typeName() == values:
            logger.info(f"Column {key} is of type {values}.")
            validation_results["passed"].append(f"Column {key} is of type {values}.")
        else:
            logger.warning(f"Data type missmatch of column {key}.")
            validation_results["failed"].append(f"Data type missmatch of column {key}.")

