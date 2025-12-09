"""
Store locations table generation module.

Creates store location records with addresses, state mappings, and operational attributes.
"""

import sys
from typing import List, Tuple
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

try:
    from utils import get_spark, write_parquet
except ImportError:
    sys.path.insert(0, '.')
    from utils import get_spark, write_parquet


# Constants
DEFAULT_OUTPUT_ROOT = "./output_parquet"
DEFAULT_NUM_LOCATIONS = 200


def create_locations_dataframe(
    spark: SparkSession,
    num_locations: int,
    states_df: DataFrame
) -> DataFrame:
    """
    Create store locations DataFrame.
    
    Args:
        spark: Active SparkSession
        num_locations: Number of locations to generate
        states_df: DataFrame containing state information
        
    Returns:
        DataFrame with store locations data
        
    Raises:
        ValueError: If parameters are invalid
    """
    if spark is None:
        raise ValueError("SparkSession cannot be None")
    
    if num_locations <= 0:
        raise ValueError("num_locations must be greater than 0")
    
    if states_df is None or states_df.rdd.isEmpty():
        raise ValueError("states_df cannot be None or empty")
    
    # Get state count for modulo operation
    num_states = states_df.count()
    
    # Generate location records
    location_rows: List[Tuple] = []
    for i in range(1, num_locations + 1):
        store_number = f"SF-{i:05d}"
        store_name = f"Spicy Fiesta #{i:05d}"
        address = f"{100 + i} Main St"
        city = f"City{i % 100}"
        zip_code = f"{90000 + (i % 1000)}"
        state_id = int((i % num_states) + 1)
        
        location_rows.append((
            store_number, store_name, address, city, state_id, zip_code
        ))
    
    # Create DataFrame
    locations_df = spark.createDataFrame(
        location_rows,
        ["StoreNumber", "StoreName", "AddressLine1", "City", "StateID", "ZipCode"]
    )
    
    # Add additional columns
    locations_df = (
        locations_df
        .withColumn("LocationID", (F.monotonically_increasing_id() + 1).cast(IntegerType()))
        .withColumn("LocationGUID", F.expr("uuid()"))
        .withColumn("IsActive", F.lit(1))
        .withColumn("CreatedDate", F.current_timestamp())
        .withColumn("ModifiedDate", F.current_timestamp())
        .withColumn("StoreType", F.lit("Standalone"))
        .withColumn("HasDriveThru", F.lit(1))
        .withColumn("HasDineIn", F.lit(1))
        .withColumn("OpeningDateID", F.lit(1).cast(IntegerType()))
        .withColumn("ClosingDateID", F.lit(None).cast(IntegerType()))
        .withColumn("PhoneNumber", F.lit("(555)000-0000"))
        .withColumn("CreatedBy", F.lit("system"))
        .withColumn("ModifiedBy", F.lit("system"))
    )
    
    # Select columns in final order
    column_order = [
        "LocationID", "LocationGUID", "StoreNumber", "StoreName", "AddressLine1",
        "City", "StateID", "ZipCode", "PhoneNumber", "StoreType", "HasDriveThru",
        "HasDineIn", "OpeningDateID", "ClosingDateID", "IsActive", "CreatedDate",
        "ModifiedDate", "CreatedBy", "ModifiedBy"
    ]
    
    return locations_df.select(*column_order)


def main(output_root: str = DEFAULT_OUTPUT_ROOT, num_locations: int = DEFAULT_NUM_LOCATIONS) -> None:
    """
    Generate store locations table and write to Parquet.
    
    Args:
        output_root: Root directory for output Parquet files
        num_locations: Number of locations to generate
        
    Raises:
        ValueError: If parameters are invalid
        Exception: If data generation or writing fails
    """
    if not output_root:
        raise ValueError("output_root must be a non-empty string")
    
    if num_locations <= 0:
        raise ValueError("num_locations must be greater than 0")
    
    spark = None
    try:
        spark = get_spark("locations")
        
        # Read states dimension
        states_df = spark.read.parquet(f"{output_root}/store.States").select("StateID", "StateCode")
        
        # Create locations
        locations_df = create_locations_dataframe(spark, num_locations, states_df)
        
        output_path = f"{output_root}/store.Locations"
        write_parquet(locations_df, output_path)
        
        print(f"Store locations table successfully written to {output_path}")
        print(f"Total locations created: {locations_df.count()}")
        
    except Exception as e:
        print(f"Error generating store locations: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT
    num_locs = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_NUM_LOCATIONS
    main(output_path, num_locs)
