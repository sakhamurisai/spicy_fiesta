# 07_menu_categories_items_recipes.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("menu")
    categories = [("CAT-01","Tacos"),("CAT-02","Burritos"),("CAT-03","Sides"),("CAT-04","Drinks")]
    cat_df = spark.createDataFrame(categories, ["CategoryCode","CategoryName"]) \
                  .withColumn("CategoryID", F.monotonically_increasing_id()+1) \
                  .withColumn("IsActive", F.lit(1)).withColumn("CreatedDate", F.current_timestamp()) \
                  .select("CategoryID","CategoryCode","CategoryName","IsActive","CreatedDate")
    write_parquet(cat_df, f"{output_root}/menu.Categories")

    # Items (300)
    items = []
    for i in range(1,301):
        code = f"ITM-{i:04d}"
        name = f"Menu Item {i:04d}"
        cat = ((i-1) % 4) + 1
        base = round(1.99 + (i % 10) * 0.75,2)
        items.append((code,name,cat,base))
    items_df = spark.createDataFrame(items, ["ItemCode","ItemName","CategoryID","BasePrice"]) \
             .withColumn("ItemID", F.monotonically_increasing_id()+1) \
             .withColumn("IsActive", F.lit(1)) \
             .withColumn("LaunchDateID", F.lit(10000)) \
             .withColumn("CreatedDate", F.current_timestamp()) \
             .select("ItemID","ItemCode","ItemName","CategoryID","BasePrice","IsActive","LaunchDateID","CreatedDate")
    write_parquet(items_df, f"{output_root}/menu.Items")

    # Ingredients (raw materials mapped to inv.Items later)
    ingredients = [("ING-01","Flour", "lb"),("ING-02","Beef","lb"),("ING-03","Cheese","lb"),("ING-04","Tomato","lb"),("ING-05","Lettuce","lb"),("ING-06","Oil","gal"),("ING-07","Soda Syrup","gal")]
    ing_df = spark.createDataFrame(ingredients, ["IngredientCode","IngredientName","UnitOfMeasure"]) \
                .withColumn("IngredientID", F.monotonically_increasing_id()+1) \
                .withColumn("CreatedDate", F.current_timestamp()) \
                .select("IngredientID","IngredientCode","IngredientName","UnitOfMeasure","CreatedDate")
    write_parquet(ing_df, f"{output_root}/menu.Ingredients")

    # Recipes: map each menu item to 2-4 ingredients with quantities
    import random
    rows = []
    item_ids = [r.ItemID for r in items_df.select("ItemID").collect()]
    ing_ids = [r.IngredientID for r in ing_df.select("IngredientID").collect()]
    for it in item_ids:
        k = random.randint(2,4)
        picks = random.sample(ing_ids, k)
        for pid in picks:
            qty = round(random.uniform(0.05, 2.0),3)  # quantity in unit of ingredient's UoM
            rows.append((it, pid, qty))
    rec_df = spark.createDataFrame(rows, ["ItemID","IngredientID","Quantity"]) \
                 .withColumn("RecipeItemID", F.monotonically_increasing_id()+1) \
                 .withColumn("CreatedDate", F.current_timestamp())
    write_parquet(rec_df, f"{output_root}/menu.RecipeItems")

    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
