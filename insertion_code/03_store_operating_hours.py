# 03_store_operating_hours.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("operating_hours")
    locs = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID").cache()
    rows = []
    for loc in locs.collect():
        lid = loc.LocationID
        for dow in range(1,8):
            rows.append((lid, dow, "08:00:00", "22:00:00", 0, None, 1, None))
    df = spark.createDataFrame(rows, ["LocationID","DayOfWeek","OpenTime","CloseTime","IsHoliday","HolidayName","EffectiveDateID","ExpiryDateID"]) \
              .withColumn("OperatingHoursID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp())
    df = df.select("OperatingHoursID","LocationID","DayOfWeek","OpenTime","CloseTime","IsHoliday","HolidayName","EffectiveDateID","ExpiryDateID","CreatedDate")
    write_parquet(df, f"{output_root}/store.OperatingHours")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
