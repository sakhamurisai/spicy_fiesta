"""Loyalty members and rewards generation module."""
from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)


def main(n_members=2000):
    """Generate loyalty tables."""
    spark = get_spark("loyalty")
    configure_azure_blob_storage(spark)
    
    try:
        azure_path = get_azure_blob_path("loyalty")
        
        rows = []
        for i in range(1, n_members+1):
            rows.append((f"MBR-{i:07d}", f"Member{i}", f"Last{i}", f"member{i}@example.com", f"(555){i%1000:03d}-{i%10000:04d}"))
        df = spark.createDataFrame(rows, ["MemberNumber","FirstName","LastName","Email","PhoneNumber"]) \
                  .withColumn("MemberID", F.monotonically_increasing_id()+1) \
                  .withColumn("EnrollmentDateID", F.lit(20000)) \
                  .withColumn("MembershipTier", F.lit("Bronze")) \
                  .withColumn("TotalPoints", F.lit(0)) \
                  .withColumn("CreatedDate", F.current_timestamp())
        write_parquet(df.select("MemberID","MemberNumber","FirstName","LastName","Email","PhoneNumber","EnrollmentDateID","MembershipTier","TotalPoints","CreatedDate"),
                      azure_path)
        
        rewards = [("RW-01","Free Taco",100),("RW-02","$3 off",200)]
        rdf = spark.createDataFrame(rewards, ["RewardCode","RewardName","PointsCost"]) \
                   .withColumn("RewardID", F.monotonically_increasing_id()+1) \
                   .withColumn("IsActive", F.lit(1)).withColumn("StartDateID", F.lit(20000)).withColumn("CreatedDate", F.current_timestamp())
        write_parquet(rdf.select("RewardID","RewardCode","RewardName","PointsCost","IsActive","StartDateID","CreatedDate"), azure_path)
        
        logger.info("Loyalty tables created successfully")
        
    except Exception as e:
        logger.error(f"Error generating loyalty: {str(e)}")
        raiseif __name__ == "__main__":
    main()