# utils.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import pyspark.sql.functions as F

def get_spark(app_name="spicy-fiesta"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    # tune default shuffle partitions for large jobs if needed
    spark.conf.set("spark.sql.shuffle.partitions", "400")
    return spark

def write_parquet(df, path, partitionBy=None):
    if partitionBy:
        df.write.mode("overwrite").partitionBy(partitionBy).parquet(path)
    else:
        df.write.mode("overwrite").parquet(path)
