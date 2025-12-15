from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

POSITIONS_DATA: List[Tuple] = [
    ("POS-01", "Crew Member", 1, "Operations", 10.0, 15.0, 0),
    ("POS-02", "Shift Supervisor", 2, "Operations", 14.0, 20.0, 0),
    ("POS-03", "Manager", 3, "Management", 22.0, 40.0, 1),
    ("POS-04", "District Manager", 4, "Management", 40.0, 65.0, 1),
]


def create_positions_dataframe(spark):
    """Create positions DataFrame."""
    if spark is None:
        raise ValueError("SparkSession cannot be None")
    
    positions_df = spark.createDataFrame(
        POSITIONS_DATA,
        ["PositionCode", "PositionName", "PositionLevel", "Department",
         "MinHourlyRate", "MaxHourlyRate", "RequiresCertification"]
    )
    
    positions_df = (
        positions_df
        .withColumn("PositionID", (F.monotonically_increasing_id() + 1).cast("int"))
        .withColumn("IsActive", F.lit(1))
        .withColumn("CreatedDate", F.current_timestamp())
    )
    
    column_order = [
        "PositionID", "PositionCode", "PositionName", "PositionLevel",
        "Department", "MinHourlyRate", "MaxHourlyRate", "RequiresCertification",
        "IsActive", "CreatedDate"
    ]
    
    return positions_df.select(*column_order)


def main():
    """Generate positions table and write to Azure."""
    spark = get_spark("positions")
    configure_azure_blob_storage(spark)
    
    try:
        positions_df = create_positions_dataframe(spark)
        
        azure_path = get_azure_blob_path("emp")
        write_parquet(positions_df, azure_path)
        
        logger.info(f"Positions table successfully written. Total positions: {positions_df.count()}")
        
    except Exception as e:
        logger.error(f"Error generating positions: {str(e)}")
        raise
if __name__ == "__main__":
    main()