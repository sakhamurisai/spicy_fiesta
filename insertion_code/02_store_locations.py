"""
Store Locations data generator.

Generates store location data with proper foreign key references to states.
"""
from utils import get_spark, write_parquet, validate_output_path
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import logging

logger = logging.getLogger(__name__)


def create_locations_dataframe(spark, output_root: str, n_locations: int = 200):
    """
    Create DataFrame with store location data.
    
    Args:
        spark: SparkSession instance
        output_root: Path to read states data
        n_locations: Number of locations to generate
        
    Returns:
        DataFrame with location information
    """
    logger.info(f"Creating {n_locations} store locations")
    
    # Load states for valid StateID references
    states_df = spark.read.parquet(f"{output_root}/store.States") \
                     .select("StateID", "StateCode")
    
    state_count = states_df.count()
    if state_count == 0:
        raise ValueError("No states found. Please run 01_store_states.py first")
    
    # Generate locations using Spark operations (avoid collect for large datasets)
    locations = spark.range(1, n_locations + 1).toDF("LocationSeq")
    
    locations = locations \
        .withColumn("StoreNumber", F.concat(F.lit("SF-"), F.lpad(F.col("LocationSeq").cast("string"), 5, "0"))) \
        .withColumn("StoreName", F.concat(F.lit("Spicy Fiesta #"), F.lpad(F.col("LocationSeq").cast("string"), 5, "0"))) \
        .withColumn("AddressLine1", F.concat(F.col("LocationSeq") + 100, F.lit(" Main St"))) \
        .withColumn("City", F.concat(F.lit("City"), (F.col("LocationSeq") % 100).cast("string"))) \
        .withColumn("StateID", ((F.col("LocationSeq") % state_count) + 1).cast("int")) \
        .withColumn("ZipCode", F.lpad((90000 + (F.col("LocationSeq") % 1000)).cast("string"), 5, "0"))
    
    # Add required columns
    window_spec = Window.orderBy("LocationSeq")
    locations = locations \
        .withColumn("LocationID", row_number().over(window_spec)) \
        .withColumn("LocationGUID", F.expr("uuid()")) \
        .withColumn("IsActive", F.lit(1)) \
        .withColumn("CreatedDate", F.current_timestamp()) \
        .withColumn("ModifiedDate", F.current_timestamp()) \
        .withColumn("StoreType", F.lit("Standalone")) \
        .withColumn("HasDriveThru", F.lit(1)) \
        .withColumn("HasDineIn", F.lit(1)) \
        .withColumn("OpeningDateID", F.lit(1)) \
        .withColumn("ClosingDateID", F.lit(None).cast("int")) \
        .withColumn("PhoneNumber", F.lit("(555)000-0000")) \
        .withColumn("CreatedBy", F.lit("system")) \
        .withColumn("ModifiedBy", F.lit("system"))
    
    # Select final columns in order
    columns = [
        "LocationID", "LocationGUID", "StoreNumber", "StoreName",
        "AddressLine1", "City", "StateID", "ZipCode", "PhoneNumber",
        "StoreType", "HasDriveThru", "HasDineIn", "OpeningDateID",
        "ClosingDateID", "IsActive", "CreatedDate", "ModifiedDate",
        "CreatedBy", "ModifiedBy"
    ]
    
    return locations.select(*columns)


def validate_locations_data(df, expected_count: int):
    """
    Validate locations DataFrame for data quality.
    
    Args:
        df: Locations DataFrame to validate
        expected_count: Expected number of locations
        
    Raises:
        ValueError: If validation fails
    """
    count = df.count()
    if count != expected_count:
        raise ValueError(f"Expected {expected_count} locations, got {count}")
    
    # Check for duplicate store numbers
    distinct_numbers = df.select("StoreNumber").distinct().count()
    if distinct_numbers != count:
        raise ValueError("Duplicate store numbers found")
    
    # Validate StateID is positive
    invalid_states = df.filter(F.col("StateID") <= 0).count()
    if invalid_states > 0:
        raise ValueError(f"Found {invalid_states} locations with invalid StateID")
    
    logger.info("Locations data validation passed")


def main(output_root: str = "./output_parquet", n_locations: int = 200):
    """
    Main execution function for locations data generation.
    
    Args:
        output_root: Root directory for output Parquet files
        n_locations: Number of locations to generate
    """
    validate_output_path(output_root)
    
    spark = get_spark("locations")
    
    try:
        locations_df = create_locations_dataframe(spark, output_root, n_locations)
        
        # Validate before writing
        validate_locations_data(locations_df, n_locations)
        
        # Write to Parquet
        write_parquet(locations_df, f"{output_root}/store.Locations")
        
        logger.info(f"Locations data generation completed: {n_locations} locations created")
        
    except Exception as e:
        logger.error(f"Failed to generate locations data: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    import sys
    output_path = sys.argv[1] if len(sys.argv) > 1 else "./output_parquet"
    n_locs = int(sys.argv[2]) if len(sys.argv) > 2 else 200
    main(output_path, n_locs)