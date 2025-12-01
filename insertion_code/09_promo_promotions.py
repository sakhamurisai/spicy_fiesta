# 09_promo_promotions.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("promos")
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
                  f"{output_root}/promo.Promotions")

    # PromotionItems: apply to random items
    items = spark.read.parquet(f"{output_root}/menu.Items").select("ItemID").limit(50)
    promo_id = df.first().PromotionID
    pi = items.withColumn("PromotionID", F.lit(promo_id)).withColumn("PromotionItemID", F.monotonically_increasing_id()+1).withColumn("CreatedDate", F.current_timestamp()) \
              .select("PromotionItemID","PromotionID","ItemID","CreatedDate")
    write_parquet(pi, f"{output_root}/promo.PromotionItems")

    # Promo stores: apply across sample stores
    locs = spark.read.parquet(f"{output_root}/store.Locations").select("LocationID").limit(200)
    ps = locs.withColumn("PromotionStoreID", F.monotonically_increasing_id()+1).withColumn("PromotionID", F.lit(promo_id)).withColumn("IsActive", F.lit(1)).withColumn("CreatedDate", F.current_timestamp()) \
             .select("PromotionStoreID","PromotionID","LocationID","IsActive","CreatedDate")
    write_parquet(ps, f"{output_root}/promo.PromotionStores")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
