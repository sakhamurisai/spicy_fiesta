"""Menu categories, items, and recipes generation module."""
from utils import get_spark, write_parquet
from azure_config import configure_azure_blob_storage, get_azure_blob_path
import pyspark.sql.functions as F
import random
import logging

logger = logging.getLogger(__name__)

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


def create_menu_dataframes(spark, seed=42):
    """Create all menu-related DataFrames."""
    if seed:
        random.seed(seed)
    
    cat_df = spark.createDataFrame(CATEGORIES_DATA, ["CategoryCode", "CategoryName"])
    cat_df = (cat_df
        .withColumn("CategoryID", F.monotonically_increasing_id() + 1)
        .withColumn("IsActive", F.lit(1))
        .withColumn("CreatedDate", F.current_timestamp())
        .select("CategoryID", "CategoryCode", "CategoryName", "IsActive", "CreatedDate")
    )
    
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
    
    ing_df = spark.createDataFrame(INGREDIENTS_DATA, ["IngredientCode", "IngredientName", "UnitOfMeasure"])
    ing_df = (ing_df
        .withColumn("IngredientID", F.monotonically_increasing_id() + 1)
        .withColumn("CreatedDate", F.current_timestamp())
        .select("IngredientID", "IngredientCode", "IngredientName", "UnitOfMeasure", "CreatedDate")
    )
    
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


def main():
    """Generate menu tables."""
    spark = get_spark("menu")
    configure_azure_blob_storage(spark)
    
    try:
        cat_df, items_df, ing_df, rec_df = create_menu_dataframes(spark)
        
        azure_path = get_azure_blob_path("menu")
        write_parquet(cat_df, azure_path)
        write_parquet(items_df, azure_path)
        write_parquet(ing_df, azure_path)
        write_parquet(rec_df, azure_path)
        
        logger.info("Menu tables created successfully")
        
    except Exception as e:
        logger.error(f"Error generating menu: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()