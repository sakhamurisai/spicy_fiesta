from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import logging

logger = logging.getLogger(__name__)

# Complete list of all 51 US states/territories
ALL_STATES = [
    ("AL", "Alabama", "South", 0.04),
    ("AK", "Alaska", "West", 0.00),
    ("AZ", "Arizona", "West", 0.056),
    ("AR", "Arkansas", "South", 0.065),
    ("CA", "California", "West", 0.0725),
    ("CO", "Colorado", "West", 0.029),
    ("CT", "Connecticut", "Northeast", 0.0635),
    ("DE", "Delaware", "Northeast", 0.00),
    ("FL", "Florida", "South", 0.06),
    ("GA", "Georgia", "South", 0.04),
    ("HI", "Hawaii", "West", 0.04),
    ("ID", "Idaho", "West", 0.06),
    ("IL", "Illinois", "Midwest", 0.0625),
    ("IN", "Indiana", "Midwest", 0.07),
    ("IA", "Iowa", "Midwest", 0.06),
    ("KS", "Kansas", "Midwest", 0.065),
    ("KY", "Kentucky", "South", 0.06),
    ("LA", "Louisiana", "South", 0.0445),
    ("ME", "Maine", "Northeast", 0.055),
    ("MD", "Maryland", "South", 0.06),
    ("MA", "Massachusetts", "Northeast", 0.0625),
    ("MI", "Michigan", "Midwest", 0.06),
    ("MN", "Minnesota", "Midwest", 0.06875),
    ("MS", "Mississippi", "South", 0.07),
    ("MO", "Missouri", "Midwest", 0.04225),
    ("MT", "Montana", "West", 0.00),
    ("NE", "Nebraska", "Midwest", 0.055),
    ("NV", "Nevada", "West", 0.0685),
    ("NH", "New Hampshire", "Northeast", 0.00),
    ("NJ", "New Jersey", "Northeast", 0.06625),
    ("NM", "New Mexico", "West", 0.05125),
    ("NY", "New York", "Northeast", 0.04),
    ("NC", "North Carolina", "South", 0.0475),
    ("ND", "North Dakota", "Midwest", 0.05),
    ("OH", "Ohio", "Midwest", 0.0575),
    ("OK", "Oklahoma", "South", 0.045),
    ("OR", "Oregon", "West", 0.00),
    ("PA", "Pennsylvania", "Northeast", 0.06),
    ("RI", "Rhode Island", "Northeast", 0.07),
    ("SC", "South Carolina", "South", 0.06),
    ("SD", "South Dakota", "Midwest", 0.045),
    ("TN", "Tennessee", "South", 0.07),
    ("TX", "Texas", "South", 0.0625),
    ("UT", "Utah", "West", 0.0485),
    ("VT", "Vermont", "Northeast", 0.06),
    ("VA", "Virginia", "South", 0.053),
    ("WA", "Washington", "West", 0.065),
    ("WV", "West Virginia", "South", 0.06),
    ("WI", "Wisconsin", "Midwest", 0.05),
    ("WY", "Wyoming", "West", 0.04),
    ("DC", "District of Columbia", "South", 0.06)
]


def create_states_dataframe(spark):
    """
    Create DataFrame with all US state data.
    
    Args:
        spark: SparkSession instance
        
    Returns:
        DataFrame with state information
    """
    logger.info(f"Creating states DataFrame with {len(ALL_STATES)} states")
    
    df = spark.createDataFrame(
        ALL_STATES,
        ["StateCode", "StateName", "StateRegion", "TaxRate"]
    )
    
    df = df.withColumn("IsActive", F.lit(1)) \
           .withColumn("CreatedDate", F.current_timestamp()) \
           .withColumn("ModifiedDate", F.current_timestamp())
    
    window_spec = Window.orderBy("StateCode")
    df = df.withColumn("StateID", row_number().over(window_spec))
    
    columns = [
        "StateID", "StateCode", "StateName", "StateRegion",
        "TaxRate", "IsActive", "CreatedDate", "ModifiedDate"
    ]
    
    return df.select(*columns)


def validate_states_data(df):
    """
    Validate states DataFrame for data quality.
    
    Args:
        df: States DataFrame to validate
        
    Raises:
        ValueError: If validation fails
    """
    count = df.count()
    if count != 51:
        raise ValueError(f"Expected 51 states, got {count}")
    
    distinct_codes = df.select("StateCode").distinct().count()
    if distinct_codes != count:
        raise ValueError("Duplicate state codes found")
    
    invalid_rates = df.filter(F.col("TaxRate") < 0).count()
    if invalid_rates > 0:
        raise ValueError(f"Found {invalid_rates} states with negative tax rates")
    
    logger.info("States data validation passed")


def main():
    """
    Main execution function for states data generation.
    """
    spark = get_spark("states")
    configure_azure_blob_storage(spark)
    
    try:
        states_df = create_states_dataframe(spark)
        validate_states_data(states_df)
        
        azure_path = get_azure_blob_path("store")
        write_parquet(states_df, azure_path)
        
        logger.info("States data generation completed successfully")
        
    except Exception as e:
        logger.error(f"Failed to generate states data: {e}")
        raise
if __name__ == "__main__":
    main()