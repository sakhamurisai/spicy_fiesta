"""Employee records generation module."""
from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

DEFAULT_NUM_EMPLOYEES = 1000


def create_employees_dataframe(spark, num_employees: int, positions_df, locations_df):
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


def main(num_employees: int = DEFAULT_NUM_EMPLOYEES):
    """Generate employees table."""
    spark = get_spark("employees")
    configure_azure_blob_storage(spark)
    
    try:
        azure_emp_path = get_azure_blob_path("emp")
        azure_store_path = get_azure_blob_path("store")

        positions_df = spark.read.parquet(f"{azure_emp_path}/positions").select("PositionID")
        locations_df = spark.read.parquet(f"{azure_store_path}/locations").select("LocationID")
        employees_df = create_employees_dataframe(spark, num_employees, positions_df, locations_df)
        write_parquet(employees_df, f"{azure_emp_path}/employees")
        logger.info(f"Employees created: {employees_df.count()}")
        
    except Exception as e:
        logger.error(f"Error generating employees: {str(e)}")
        raise
if __name__ == "__main__":
    main()