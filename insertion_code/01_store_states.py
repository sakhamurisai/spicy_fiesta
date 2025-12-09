"""
Store states dimension table generation module.

Creates the states dimension table with state codes, names, regions, and tax rates
for all 50 US states plus DC.
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


# Constants
DEFAULT_OUTPUT_ROOT = "./output_parquet"

# Complete US states data with tax rates
US_STATES_DATA: List[Tuple[str, str, str, float]] = [
    ("AL", "Alabama", "South", 0.04),
    ("AK", "Alaska", "West", 0.00),  # no state sales tax
    ("AZ", "Arizona", "West", 0.056),
    ("AR", "Arkansas", "South", 0.065),
    ("CA", "California", "West", 0.0725),
    ("CO", "Colorado", "West", 0.029),
    ("CT", "Connecticut", "Northeast", 0.0635),
    ("DE", "Delaware", "Northeast", 0.00),  # no state sales tax
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
    ("MT", "Montana", "West", 0.00),  # no state sales tax
    ("NE", "Nebraska", "Midwest", 0.055),
    ("NV", "Nevada", "West", 0.0685),
    ("NH", "New Hampshire", "Northeast", 0.00),  # no state sales tax
    ("NJ", "New Jersey", "Northeast", 0.06625),
    ("NM", "New Mexico", "West", 0.05125),
    ("NY", "New York", "Northeast", 0.04),
    ("NC", "North Carolina", "South", 0.0475),
    ("ND", "North Dakota", "Midwest", 0.05),
    ("OH", "Ohio", "Midwest", 0.0575),
    ("OK", "Oklahoma", "South", 0.045),
    ("OR", "Oregon", "West", 0.00),  # no state sales tax
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


def create_states_dataframe(spark: SparkSession) -> DataFrame:
    """
    Create states dimension DataFrame with all US states.
    
    Args:
        spark: Active SparkSession
        
    Returns:
        DataFrame with states dimension data
        
    Raises:
        ValueError: If spark session is None
    """
    if spark is None:
        raise ValueError("SparkSession cannot be None")
    
    # Create DataFrame from states data
    states_df = spark.createDataFrame(
        US_STATES_DATA,
        ["StateCode", "StateName", "StateRegion", "TaxRate"]
    )
    
    # Add metadata columns
    states_df = (
        states_df
        .withColumn("IsActive", F.lit(1))
        .withColumn("CreatedDate", F.current_timestamp())
        .withColumn("ModifiedDate", F.current_timestamp())
    )
    
    # Add StateID using monotonically_increasing_id
    states_df = states_df.withColumn(
        "StateID",
        (F.monotonically_increasing_id() + 1).cast("int")
    )
    
    # Select columns in final order
    column_order = [
        "StateID", "StateCode", "StateName", "StateRegion", "TaxRate",
        "IsActive", "CreatedDate", "ModifiedDate"
    ]
    
    return states_df.select(*column_order)


def main(output_root: str = DEFAULT_OUTPUT_ROOT) -> None:
    """
    Generate states dimension table and write to Parquet.
    
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
        spark = get_spark("states")
        states_df = create_states_dataframe(spark)
        
        output_path = f"{output_root}/store.States"
        write_parquet(states_df, output_path)
        
        print(f"States dimension table successfully written to {output_path}")
        print(f"Total states created: {states_df.count()}")
        
    except Exception as e:
        print(f"Error generating states dimension: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT
    main(output_path)
