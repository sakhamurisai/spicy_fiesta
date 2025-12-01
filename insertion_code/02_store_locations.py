# 02_store_locations.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

def main(output_root="./output_parquet", n=200):
    spark = get_spark("locations")
    states = spark.read.parquet(f"{output_root}/store.States").select("StateID","StateCode").cache()
    import random
    rows = []
    for i in range(1, n+1):
        store_number = f"SF-{i:05d}"
        name = f"Spicy Fiesta #{i:05d}"
        address = f"{100+i} Main St"
        city = f"City{i%100}"
        zipc = f"{90000 + (i%1000)}"
        state_id = int((i % states.count()) + 1)
        rows.append((store_number, name, address, city, state_id, zipc))
    df = spark.createDataFrame(rows, ["StoreNumber","StoreName","AddressLine1","City","StateID","ZipCode"]) \
              .withColumn("LocationID", (F.monotonically_increasing_id()+1).cast(IntegerType())) \
              .withColumn("LocationGUID", F.expr("uuid()")) \
              .withColumn("IsActive", F.lit(1)) \
              .withColumn("CreatedDate", F.current_timestamp()) \
              .withColumn("ModifiedDate", F.current_timestamp()) \
              .withColumn("StoreType", F.lit("Standalone")) \
              .withColumn("HasDriveThru", F.lit(1)) \
              .withColumn("HasDineIn", F.lit(1)) \
              .withColumn("OpeningDateID", F.lit(1).cast(IntegerType())) \
              .withColumn("ClosingDateID", F.lit(None).cast(IntegerType())) \
              .withColumn("PhoneNumber", F.lit("(555)000-0000")) \
              .withColumn("CreatedBy", F.lit("system")) \
              .withColumn("ModifiedBy", F.lit("system"))
    cols = ["LocationID","LocationGUID","StoreNumber","StoreName","AddressLine1","City","StateID","ZipCode","PhoneNumber","StoreType","HasDriveThru","HasDineIn","OpeningDateID","ClosingDateID","IsActive","CreatedDate","ModifiedDate","CreatedBy","ModifiedBy"]
    df = df.select(*cols)
    write_parquet(df, f"{output_root}/store.Locations")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    n = int(sys.argv[2]) if len(sys.argv)>2 else 200
    main(out,n)
