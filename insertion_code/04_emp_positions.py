"""
Employee positions table generation module.

Creates position/role records with salary ranges and requirements.
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

POSITIONS_DATA: List[Tuple] = [
    ("POS-01", "Crew Member", 1, "Operations", 10.0, 15.0, 0),
    ("POS-02", "Shift Supervisor", 2, "Operations", 14.0, 20.0, 0),
    ("POS-03", "Manager", 3, "Management", 22.0, 40.0, 1),
    ("POS-04", "District Manager", 4, "Management", 40.0, 65.0, 1),
]


def create_positions_dataframe(spark: SparkSession) -> DataFrame:
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


def main(output_root: str = DEFAULT_OUTPUT_ROOT) -> None:
    """Generate positions table and write to Parquet."""
    if not output_root:
        raise ValueError("output_root must be a non-empty string")
    
    spark = None
    try:
        spark = get_spark("positions")
        positions_df = create_positions_dataframe(spark)
        
        output_path = f"{output_root}/emp.Positions"
        write_parquet(positions_df, output_path)
        
        print(f"Positions table successfully written to {output_path}")
        print(f"Total positions created: {positions_df.count()}")
        
    except Exception as e:
        print(f"Error generating positions: {str(e)}")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    output_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT
    main(output_path)
