files = dbutils.fs.ls("/mnt/restaurant_input/menu/")

rows = [(f.path,f.name,f.size) for f in files]

df = spark.createDataFrame(rows, ["path","name","size"])
display(df)

selected_files = [
    f.path for f in files if f.name.endswith(".parquet") and f.name.startswith("part-")
]
print(selected_files)
for file in selected_files:
    new_df = spark.read.parquet(file)
    display(new_df)