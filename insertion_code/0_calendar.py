"""
Calendar dimension table generation module.

Creates a comprehensive calendar/date dimension table for years 1980-2080,
including standard date attributes, fiscal calendar, and boolean flags.
"""

import sys
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Import utility functions
try:
    from utils import get_spark, write_parquet
except ImportError:
    # Fallback for testing or standalone execution
    sys.path.insert(0, '.')
    from utils import get_spark, write_parquet


# Constants
START_DATE = "1980-01-01"
END_DATE = "2080-12-31"
DEFAULT_OUTPUT_ROOT = "./output_parquet"


def create_calendar_dataframe(spark: SparkSession) -> DataFrame:
    """
    Create calendar dimension DataFrame with all date attributes.
    
    Args:
        spark: Active SparkSession
        
    Returns:
        DataFrame with complete calendar dimension data
        
    Raises:
        ValueError: If spark session is None
    """
    if spark is None:
        raise ValueError("SparkSession cannot be None")
    
    # Generate date sequence
    date_range_df = spark.sql(
        f"SELECT sequence(to_date('{START_DATE}'), "
        f"to_date('{END_DATE}'), interval 1 day) as date_sequence"
    )
    
    # Explode to individual dates
    calendar_df = date_range_df.select(
        F.explode(F.col("date_sequence")).alias("CalendarDate")
    )
    
    # Add standard date attributes
    calendar_df = (
        calendar_df
        .withColumn("Year", F.year("CalendarDate"))
        .withColumn("Quarter", F.quarter("CalendarDate"))
        .withColumn("Month", F.month("CalendarDate"))
        .withColumn("MonthName", F.date_format("CalendarDate", "MMMM"))
        .withColumn("Day", F.dayofmonth("CalendarDate"))
        .withColumn("DayOfWeek", F.date_format("CalendarDate", "u").cast("int"))
        .withColumn("DayName", F.date_format("CalendarDate", "EEEE"))
        .withColumn("WeekOfYear", F.weekofyear("CalendarDate"))
        .withColumn("ISOYear", F.year("CalendarDate"))
        .withColumn("ISOWeek", F.weekofyear("CalendarDate"))
        .withColumn("DayOfYear", F.dayofyear("CalendarDate"))
    )
    
    # Add boolean flags
    calendar_df = (
        calendar_df
        .withColumn(
            "IsWeekend",
            F.when(F.col("DayOfWeek").isin([6, 7]), 1).otherwise(0)
        )
        .withColumn("IsHoliday", F.lit(0))
        .withColumn("HolidayName", F.lit(None).cast("string"))
        .withColumn(
            "IsMonthStart",
            F.when(F.dayofmonth("CalendarDate") == 1, 1).otherwise(0)
        )
        .withColumn(
            "IsMonthEnd",
            F.when(
                F.last_day("CalendarDate") == F.col("CalendarDate"), 1
            ).otherwise(0)
        )
        .withColumn("IsQuarterStart", F.lit(0))  # Simplified for now
        .withColumn("IsQuarterEnd", F.lit(0))    # Simplified for now
        .withColumn(
            "IsYearStart",
            F.when(
                (F.dayofmonth("CalendarDate") == 1) &
                (F.month("CalendarDate") == 1),
                1
            ).otherwise(0)
        )
        .withColumn(
            "IsYearEnd",
            F.when(
                (F.dayofmonth("CalendarDate") == 31) &
                (F.month("CalendarDate") == 12),
                1
            ).otherwise(0)
        )
    )
    
    # Add fiscal calendar attributes (fiscal year starts in April - Q2)
    calendar_df = (
        calendar_df
        .withColumn("FiscalYear", F.year(F.add_months("CalendarDate", 3)))
        .withColumn("FiscalMonth", F.month(F.add_months("CalendarDate", 3)))
        .withColumn("FiscalQuarter", F.quarter(F.add_months("CalendarDate", 3)))
        .withColumn("CreatedDate", F.current_timestamp())
    )
    
    # Add CalendarID using row_number ordered by CalendarDate
    window_spec = Window.orderBy("CalendarDate")
    calendar_df = calendar_df.withColumn(
        "CalendarID",
        F.row_number().over(window_spec)
    )
    
    # Select columns in defined order
    column_order = [
        "CalendarID", "CalendarDate", "Year", "Quarter", "Month", "MonthName",
        "Day", "DayOfWeek", "DayName", "WeekOfYear", "ISOYear", "ISOWeek",
        "IsWeekend", "IsHoliday", "HolidayName", "IsMonthStart", "IsMonthEnd",
        "IsQuarterStart", "IsQuarterEnd", "IsYearStart", "IsYearEnd",
        "FiscalYear", "FiscalMonth", "FiscalQuarter", "DayOfYear", "CreatedDate"
    ]
    
    return calendar_df.select(*column_order)


def main(output_root: str = DEFAULT_OUTPUT_ROOT) -> None:
    """
    Generate calendar dimension table and write to Parquet.
    
    Args:
        output_root: Root directory for output Parquet files
        
    Raises:
        ValueError: If output_root is invalid
        Exception: If data generation or writing fails
    """
    if not output_root:
        raise ValueError("output_root must be a non-empty string")
    
    spark = None
    try:
        spark = get_spark("calendar")
        calendar_df = create_calendar_dataframe(spark)
        
        output_path = f"{output_root}/dim.Calendar"
        write_parquet(calendar_df, output_path, partitionBy="Year")
        
        print(f"Calendar dimension table successfully written to {output_path}")
        
    except Exception as e:
        print(f"Error generating calendar dimension: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT
    main(output_path)
