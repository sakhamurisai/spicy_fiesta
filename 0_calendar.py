from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import logging

logger = logging.getLogger(__name__)


def create_calendar_dimension(
    spark,
    start_date: str = "1980-01-01",
    end_date: str = "2080-12-31"
):
    """
    Create calendar dimension with date attributes.

    Args:
        spark: SparkSession instance
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        DataFrame with calendar dimension
    """
    if spark is None:
        raise ValueError("SparkSession cannot be None")

    logger.info(f"Creating calendar dimension from {start_date} to {end_date}")
    
    # Generate date sequence
    df = spark.sql(
        f"SELECT sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day) as dseq"
    )
    cal = df.select(F.explode(F.col("dseq")).alias("CalendarDate"))
    
    # Add date attributes
    cal = cal.withColumn("Year", F.year("CalendarDate")) \
             .withColumn("Quarter", F.quarter("CalendarDate")) \
             .withColumn("Month", F.month("CalendarDate")) \
             .withColumn("MonthName", F.date_format("CalendarDate", "MMMM")) \
             .withColumn("Day", F.dayofmonth("CalendarDate")) \
             .withColumn("DayOfWeek",
                        F.when(F.dayofweek("CalendarDate") == 1, 7)
                         .otherwise(F.dayofweek("CalendarDate") - 1)) \
             .withColumn("DayName", F.date_format("CalendarDate", "EEEE")) \
             .withColumn("WeekOfYear", F.weekofyear("CalendarDate")) \
             .withColumn("ISOYear", F.year("CalendarDate")) \
             .withColumn("ISOWeek", F.weekofyear("CalendarDate")) \
             .withColumn("IsWeekend", F.when(F.col("DayOfWeek").isin(6, 7), 1).otherwise(0)) \
             .withColumn("IsHoliday", F.lit(0)) \
             .withColumn("HolidayName", F.lit(None).cast("string")) \
             .withColumn("IsMonthStart", F.when(F.dayofmonth("CalendarDate") == 1, 1).otherwise(0)) \
             .withColumn("IsMonthEnd", F.when(F.last_day("CalendarDate") == F.col("CalendarDate"), 1).otherwise(0)) \
             .withColumn("IsQuarterStart", F.lit(0)) \
             .withColumn("IsQuarterEnd", F.lit(0)) \
             .withColumn("IsYearStart", F.when(
                 (F.dayofmonth("CalendarDate") == 1) & (F.month("CalendarDate") == 1), 1
             ).otherwise(0)) \
             .withColumn("IsYearEnd", F.when(
                 (F.dayofmonth("CalendarDate") == 31) & (F.month("CalendarDate") == 12), 1
             ).otherwise(0)) \
             .withColumn("FiscalYear",
                        F.when(F.month("CalendarDate") >= 4, F.year("CalendarDate") + 1)
                         .otherwise(F.year("CalendarDate"))) \
             .withColumn("FiscalMonth", F.month(F.add_months("CalendarDate", 3))) \
             .withColumn("FiscalQuarter", F.quarter(F.add_months("CalendarDate", 3))) \
             .withColumn("DayOfYear", F.dayofyear("CalendarDate")) \
             .withColumn("CreatedDate", F.current_timestamp())
    
    # Add sequential CalendarID
    window_spec = Window.orderBy(F.col("CalendarDate"))
    cal = cal.withColumn("CalendarID", row_number().over(window_spec))
    
    # Reorder columns for output
    columns = [
        "CalendarID", "CalendarDate", "Year", "Quarter", "Month", "MonthName", "Day",
        "DayOfWeek", "DayName", "WeekOfYear", "ISOYear", "ISOWeek", "IsWeekend",
        "IsHoliday", "HolidayName", "IsMonthStart", "IsMonthEnd", "IsQuarterStart",
        "IsQuarterEnd", "IsYearStart", "IsYearEnd", "FiscalYear", "FiscalMonth",
        "FiscalQuarter", "DayOfYear", "CreatedDate"
    ]
    
    return cal.select(*columns)


def main():
    """
    Main execution function for calendar dimension generation.
    Designed for Databricks environment.
    """
    spark = get_spark("calendar")
    configure_azure_blob_storage(spark)
    
    try:
        calendar_df = create_calendar_dimension(spark)
        
        # Validate output
        row_count = calendar_df.count()
        logger.info(f"Generated {row_count} calendar records")

        # Write to Azure Blob Storage
        azure_path = get_azure_blob_path("dim")
        write_parquet(calendar_df, azure_path, partitionBy="Year")
        
        logger.info("Calendar dimension generation completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to generate calendar dimension: {e}")
        raiseif __name__ == "__main__":
    main()


# Backwards-compatible alias for older tests
def create_calendar_dataframe(spark, start_date: str = "1980-01-01", end_date: str = "2080-12-31"):
    """Compatibility wrapper used by test suite expecting this function name."""
    return create_calendar_dimension(spark, start_date, end_date)