# 04_emp_positions.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("positions")
    rows = [
        ("POS-01","Crew Member",1,"Operations",10.0,15.0,0),
        ("POS-02","Shift Supervisor",2,"Operations",14.0,20.0,0),
        ("POS-03","Manager",3,"Management",22.0,40.0,1),
        ("POS-04","District Manager",4,"Management",40.0,65.0,1),
    ]
    df = spark.createDataFrame(rows, ["PositionCode","PositionName","PositionLevel","Department","MinHourlyRate","MaxHourlyRate","RequiresCertification"]) \
              .withColumn("PositionID", (F.monotonically_increasing_id()+1).cast("int")) \
              .withColumn("IsActive", F.lit(1)) \
              .withColumn("CreatedDate", F.current_timestamp()) \
              .select("PositionID","PositionCode","PositionName","PositionLevel","Department","MinHourlyRate","MaxHourlyRate","RequiresCertification","IsActive","CreatedDate")
    write_parquet(df, f"{output_root}/emp.Positions")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
