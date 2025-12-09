"""Menu categories, items, and recipes generation module."""
import sys
import random
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

try:
    from utils import get_spark, write_parquet
except ImportError:
    sys.path.insert(0, '.')
    from utils import get_spark, write_parquet

DEFAULT_OUTPUT_ROOT = "./output_parquet"
NUM_ITEMS = 300

CATEGORIES_DATA = [
    ("CAT-01", "Tacos"),
    ("CAT-02", "Burritos"),
    ("CAT-03", "Sides"),
    ("CAT-04", "Drinks")
]

INGREDIENTS_DATA = [
    ("ING-01", "Flour", "lb"),
    ("ING-02", "Beef", "lb"),
    ("ING-03", "Cheese", "lb"),
    ("ING-04", "Tomato", "lb"),
    ("ING-05", "Lettuce", "lb"),
    ("ING-06", "Oil", "gal"),
    ("ING-07", "Soda Syrup", "gal")
]

def create_menu_dataframes(spark: SparkSession, seed: Optional[int] = 42) -> tuple:
    """Create all menu-related DataFrames."""
    if seed:
        random.seed(seed)
    
    # Categories
    cat_df = spark.createDataFrame(CATEGORIES_DATA, ["CategoryCode", "CategoryName"])
    cat_df = (cat_df
        .withColumn("CategoryID", F.monotonically_increasing_id() + 1)
        .withColumn("IsActive", F.lit(1))
        .withColumn("CreatedDate", F.current_timestamp())
        .select("CategoryID", "CategoryCode", "CategoryName", "IsActive", "CreatedDate")
    )
    
    # Items
    items = [(f"ITM-{i:04d}", f"Menu Item {i:04d}", ((i-1) % 4) + 1,
             round(1.99 + (i % 10) * 0.75, 2)) for i in range(1, NUM_ITEMS + 1)]
    items_df = spark.createDataFrame(items, ["ItemCode", "ItemName", "CategoryID", "BasePrice"])
    items_df = (items_df
        .withColumn("ItemID", F.monotonically_increasing_id() + 1)
        .withColumn("IsActive", F.lit(1))
        .withColumn("LaunchDateID", F.lit(10000))
        .withColumn("CreatedDate", F.current_timestamp())
        .select("ItemID", "ItemCode", "ItemName", "CategoryID", "BasePrice",
               "IsActive", "LaunchDateID", "CreatedDate")
    )
    
    # Ingredients
    ing_df = spark.createDataFrame(INGREDIENTS_DATA, ["IngredientCode", "IngredientName", "UnitOfMeasure"])
    ing_df = (ing_df
        .withColumn("IngredientID", F.monotonically_increasing_id() + 1)
        .withColumn("CreatedDate", F.current_timestamp())
        .select("IngredientID", "IngredientCode", "IngredientName", "UnitOfMeasure", "CreatedDate")
    )
    
    # Recipes
    item_ids = [r.ItemID for r in items_df.collect()]
    ing_ids = [r.IngredientID for r in ing_df.collect()]
    
    recipe_rows = []
    for item_id in item_ids:
        num_ingredients = random.randint(2, 4)
        selected_ings = random.sample(ing_ids, num_ingredients)
        for ing_id in selected_ings:
            qty = round(random.uniform(0.05, 2.0), 3)
            recipe_rows.append((item_id, ing_id, qty))
    
    rec_df = spark.createDataFrame(recipe_rows, ["ItemID", "IngredientID", "Quantity"])
    rec_df = (rec_df
        .withColumn("RecipeItemID", F.monotonically_increasing_id() + 1)
        .withColumn("CreatedDate", F.current_timestamp())
    )
    
    return cat_df, items_df, ing_df, rec_df

def main(output_root: str = DEFAULT_OUTPUT_ROOT) -> None:
    """Generate menu tables."""
    spark = None
    try:
        spark = get_spark("menu")
        cat_df, items_df, ing_df, rec_df = create_menu_dataframes(spark)
        
        write_parquet(cat_df, f"{output_root}/menu.Categories")
        write_parquet(items_df, f"{output_root}/menu.Items")
        write_parquet(ing_df, f"{output_root}/menu.Ingredients")
        write_parquet(rec_df, f"{output_root}/menu.RecipeItems")
        
        print("Menu tables created successfully")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_ROOT)
