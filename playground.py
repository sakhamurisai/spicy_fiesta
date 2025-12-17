azure_menu_path = "/mnt/restaurant_input/ord/order_items"
items = spark.read.parquet(f"{azure_menu_path}")
items.show()

order_items