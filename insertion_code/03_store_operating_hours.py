"""
Store operating hours table generation module.

Creates operating hours records for each store location across all days of the week.
"""

import sys
from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

try:
    from utils import get_spark, write_parquet
except ImportError:
    sys.path.insert(0, '.')
    from utils import get_spark, write_parquet


DEFAULT_OUTPUT_ROOT = "./output_parquet"
DAYS_PER_WEEK = 7
DEFAULT_OPEN_TIME = "08:00:00"
DEFAULT_CLOSE_TIME = "22:00:00"


def create_operating_hours_dataframe(spark: SparkSession, locations_df: DataFrame) -> DataFrame:
    """
    Create operating hours DataFrame for all locations.
    
    Args:
        spark: Active SparkSession
        locations_df: DataFrame containing location information
        
    Returns:
        DataFrame with operating hours data
    """
    if spark is None:
        raise ValueError("SparkSession cannot be None")
    
    if locations_df is None or locations_df.rdd.isEmpty():
        raise ValueError("locations_df cannot be None or empty")
    
    # Collect location IDs
    location_ids = [row.LocationID for row in locations_df.collect()]
    
    # Generate operating hours for each location and day
    hours_rows: List[Tuple] = []
    for location_id in location_ids:
        for day_of_week in range(1, DAYS_PER_WEEK + 1):
            hours_rows.append((
                location_id,
                day_of_week,
                DEFAULT_OPEN_TIME,
                DEFAULT_CLOSE_TIME,
                0,  # IsHoliday
                None,  # HolidayName
                1,  # EffectiveDateID
                None  # ExpiryDateID
            ))
    
    # Create DataFrame
    hours_df = spark.createDataFrame(
        hours_rows,
        ["LocationID", "DayOfWeek", "OpenTime", "CloseTime", "IsHoliday",
         "HolidayName", "EffectiveDateID", "ExpiryDateID"]
    )
    
    # Add operating hours ID and created date
    hours_df = (
        hours_df
        .withColumn("OperatingHoursID", F.monotonically_increasing_id() + 1)
        .withColumn("CreatedDate", F.current_timestamp())
    )
    
    # Select columns in final order
    column_order = [
        "OperatingHoursID", "LocationID", "DayOfWeek", "OpenTime", "CloseTime",
        "IsHoliday", "HolidayName", "EffectiveDateID", "ExpiryDateID", "CreatedDate"
    ]
    
    return hours_df.select(*column_order)


def main(output_root: str = DEFAULT_OUTPUT_ROOT) -> None:
    """
    Generate operating hours table and write to Parquet.
    
    Args:
        output_root: Root directory for output Parquet files
    """
    if not output_root:
        raise ValueError("output_root must be a non-empty string")
    
    spark = None
    try:
        spark = get_spark("operating_hours")
        
        # Read locations
        locations_df = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID")
        
        # Create operating hours
        hours_df = create_operating_hours_dataframe(spark, locations_df)
        
        output_path = f"{output_root}/store.OperatingHours"
        write_parquet(hours_df, output_path)
        
        print(f"Operating hours table successfully written to {output_path}")
        print(f"Total records created: {hours_df.count()}")
        
    except Exception as e:
        print(f"Error generating operating hours: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT
    main(output_path)
