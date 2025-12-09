"""Employee records generation module."""
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

DEFAULT_OUTPUT_ROOT = "./output_parquet"
DEFAULT_NUM_EMPLOYEES = 1000

def create_employees_dataframe(spark: SparkSession, num_employees: int,
                               positions_df: DataFrame, locations_df: DataFrame) -> DataFrame:
    """Create employees DataFrame."""
    if spark is None or num_employees <= 0:
        raise ValueError("Invalid parameters")
    
    position_ids = [r.PositionID for r in positions_df.collect()]
    location_ids = [r.LocationID for r in locations_df.collect()]
    
    emp_rows: List[Tuple] = []
    for i in range(1, num_employees + 1):
        emp_rows.append((
            f"EMP-{i:06d}",
            f"First{i}",
            f"Last{i}",
            "1985-01-01",
            position_ids[i % len(position_ids)],
            location_ids[i % len(location_ids)]
        ))
    
    df = spark.createDataFrame(emp_rows, ["EmployeeNumber", "FirstName", "LastName",
                                           "DateOfBirth", "PositionID", "PrimaryLocationID"])
    
    df = (df
        .withColumn("EmployeeID", (F.monotonically_increasing_id() + 1).cast(IntegerType()))
        .withColumn("EmployeeGUID", F.expr("uuid()"))
        .withColumn("Email", F.concat(F.col("FirstName"), F.lit("."),
                                      F.col("LastName"), F.lit("@spicyfiesta.com")))
        .withColumn("HireDateID", F.lit(14000).cast(IntegerType()))
        .withColumn("HourlyRate", F.lit(15.0))
        .withColumn("PasswordHash", F.lit(b"hash"))
        .withColumn("PasswordSalt", F.lit(b"salt"))
        .withColumn("IsActive", F.lit(1))
        .withColumn("CreatedDate", F.current_timestamp())
    )
    
    return df.select("EmployeeID", "EmployeeGUID", "EmployeeNumber", "FirstName", "LastName",
                    "Email", "DateOfBirth", "HireDateID", "PositionID", "PrimaryLocationID",
                    "HourlyRate", "PasswordHash", "PasswordSalt", "IsActive", "CreatedDate")

def main(output_root: str = DEFAULT_OUTPUT_ROOT, num_employees: int = DEFAULT_NUM_EMPLOYEES) -> None:
    """Generate employees table."""
    spark = None
    try:
        spark = get_spark("employees")
        positions_df = spark.read.parquet(f"{output_root}/emp.Positions").select("PositionID")
        locations_df = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID")
        
        employees_df = create_employees_dataframe(spark, num_employees, positions_df, locations_df)
        write_parquet(employees_df, f"{output_root}/emp.Employees")
        print(f"Employees created: {employees_df.count()}")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT
    n = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_NUM_EMPLOYEES
    main(out, n)
