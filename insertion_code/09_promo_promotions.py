"""Promotions generation module."""
from utils import get_spark, write_parquet
from azure_config import configure_azure_blob_storage, get_azure_blob_path
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)


def main():
    """Generate promotions tables."""
    spark = get_spark("promos")
    configure_azure_blob_storage(spark)
    
    try:
        azure_promo_path = get_azure_blob_path("promo")
        azure_menu_path = get_azure_blob_path("menu")
        azure_store_path = get_azure_blob_path("store")
        
        promos = [
            ("PROMO-10","10% off", "Percentage", 10.0, None, 20000, 30000),
            ("PROMO-5","$5 off $25", "FixedAmount", None, 5.0, 21000, 40000),
        ]
        df = spark.createDataFrame(promos, ["PromotionCode","PromotionName","PromotionType","DiscountPercentage","DiscountAmount","StartDateID","EndDateID"]) \
               .withColumn("PromotionID", F.monotonically_increasing_id()+1) \
               .withColumn("ApplicableChannels", F.lit("All")) \
               .withColumn("IsActive", F.lit(1)) \
               .withColumn("CreatedDate", F.current_timestamp())
        write_parquet(df.select("PromotionID","PromotionCode","PromotionName","PromotionType","DiscountPercentage","DiscountAmount","StartDateID","EndDateID","ApplicableChannels","IsActive","CreatedDate"),
                      azure_promo_path)
        
        items = spark.read.parquet(azure_menu_path).select("ItemID").limit(50)
        promo_id = df.first().PromotionID
        pi = items.withColumn("PromotionID", F.lit(promo_id)).withColumn("PromotionItemID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp()) \
                  .select("PromotionItemID","PromotionID","ItemID","CreatedDate")
        write_parquet(pi, azure_promo_path)
        
        locs = spark.read.parquet(azure_store_path).select("LocationID").limit(200)
        ps = locs.withColumn("PromotionStoreID", F.monotonically_increasing_id()+1).withColumn("PromotionID", F.lit(promo_id)).withColumn("IsActive", F.lit(1)).withColumn("CreatedDate", F.current_timestamp()) \
                 .select("PromotionStoreID","PromotionID","LocationID","IsActive","CreatedDate")
        write_parquet(ps, azure_promo_path)
        
        logger.info("Promotions tables created successfully")
        
    except Exception as e:
        logger.error(f"Error generating promotions: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()