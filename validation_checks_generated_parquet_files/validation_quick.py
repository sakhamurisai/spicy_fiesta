from pyspark.sql.functions import col, year, min, max

path = "/mnt/restaurant_input/dim"
df = spark.read.option("recursiveFileLookup","true").parquet(path)
new_df = df.select(year(col("CalendarDate")).alias("year")).distinct()
new_df.select(min("year"),max("year")).show()