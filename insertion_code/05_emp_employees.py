# 05_emp_employees.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

def main(output_root="./output_parquet", n=1000):
    spark = get_spark("employees")
    positions = spark.read.parquet(f"{output_root}/emp.Positions").select("PositionID").collect()
    positions_ids = [r.PositionID for r in positions]
    locs = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID").collect()
    loc_ids = [r.LocationID for r in locs]
    rows = []
    for i in range(1, n+1):
        emp_num = f"EMP-{i:06d}"
        fn = f"First{i}"
        ln = f"Last{i}"
        pos = positions_ids[i % len(positions_ids)]
        loc = loc_ids[i % len(loc_ids)]
        rows.append((emp_num, fn, ln, "1985-01-01", pos, loc))
    df = spark.createDataFrame(rows, ["EmployeeNumber","FirstName","LastName","DateOfBirth","PositionID","PrimaryLocationID"]) \
              .withColumn("EmployeeID", (F.monotonically_increasing_id()+1).cast(IntegerType())) \
              .withColumn("EmployeeGUID", F.expr("uuid()")) \
              .withColumn("Email", F.concat(F.col("FirstName"), F.lit("."), F.col("LastName"), F.lit("@spicyfiesta.com"))) \
              .withColumn("HireDateID", F.lit(14000).cast(IntegerType())) \
              .withColumn("HourlyRate", F.lit(15.0)) \
              .withColumn("PasswordHash", F.lit(b"hash")) \
              .withColumn("PasswordSalt", F.lit(b"salt")) \
              .withColumn("IsActive", F.lit(1)).withColumn("CreatedDate", F.current_timestamp())
    df = df.select("EmployeeID","EmployeeGUID","EmployeeNumber","FirstName","LastName","Email","DateOfBirth","HireDateID","PositionID","PrimaryLocationID","HourlyRate","PasswordHash","PasswordSalt","IsActive","CreatedDate")
    write_parquet(df, f"{output_root}/emp.Employees")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    n = int(sys.argv[2]) if len(sys.argv)>2 else 1000
    main(out,n)
