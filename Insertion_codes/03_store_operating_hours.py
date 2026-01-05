from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

DAYS_PER_WEEK = 7
DEFAULT_OPEN_TIME = "08:00:00"
DEFAULT_CLOSE_TIME = "22:00:00"


def create_operating_hours_dataframe(spark, locations_df):
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
    
    location_ids = [row.LocationID for row in locations_df.collect()]
    
    hours_rows: List[Tuple] = []
    for location_id in location_ids:
        for day_of_week in range(1, DAYS_PER_WEEK + 1):
            hours_rows.append((
                location_id,
                day_of_week,
                DEFAULT_OPEN_TIME,
                DEFAULT_CLOSE_TIME,
                0,
                None,
                1,
                None
            ))
    
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType
    
    schema = StructType([
        StructField("LocationID", IntegerType(), False),
        StructField("DayOfWeek", IntegerType(), False),
        StructField("OpenTime", StringType(), False),
        StructField("CloseTime", StringType(), False),
        StructField("IsHoliday", IntegerType(), False),
        StructField("HolidayName", StringType(), True),
        StructField("EffectiveDateID", IntegerType(), True),
        StructField("ExpiryDateID", IntegerType(), True)
    ])
    
    hours_df = spark.createDataFrame(hours_rows, schema)
    
    hours_df = (
        hours_df
        .withColumn("OperatingHoursID", F.monotonically_increasing_id() + 1)
        .withColumn("CreatedDate", F.current_timestamp())
    )
    
    column_order = [
        "OperatingHoursID", "LocationID", "DayOfWeek", "OpenTime", "CloseTime",
        "IsHoliday", "HolidayName", "EffectiveDateID", "ExpiryDateID", "CreatedDate"
    ]
    
    return hours_df.select(*column_order)


def main():
    """
    Generate operating hours table and write to Azure.
    """
    spark = get_spark("operating_hours")
    configure_azure_blob_storage(spark)
    
    try:
        azure_store_path = get_azure_blob_path("store")
        locations_df = spark.read.parquet(f"{azure_store_path}/locations").select("LocationID")
        hours_df = create_operating_hours_dataframe(spark, locations_df)
        write_parquet(hours_df, f"{azure_store_path}/operating_hours")
        logger.info(f"Operating hours table successfully written. Total records: {hours_df.count()}")
        
    except Exception as e:
        logger.error(f"Error generating operating hours: {str(e)}")
        raise


if __name__ == "__main__":
    main()