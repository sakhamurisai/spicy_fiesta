from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Calendar Table Insertion") \  
    .getOrCreate()

calendar_df = spark.createDataFrame([
    (1, "2024-01-01", 2024, "Q1", 1, "2023-12-31", "2024-01-06", "2024-01-01", "2024-01-31"),
    (2, "2024-01-02", 2024, "Q1", 1, "2023-12-31", "2024-01-06", "2024-01-01", "2024-01-31")