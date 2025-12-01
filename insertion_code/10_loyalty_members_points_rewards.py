# 10_loyalty_members_points_rewards.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet", n_members=2000):
    spark = get_spark("loyalty")
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
                  f"{output_root}/loyalty.Members")

    # Rewards sample
    rewards = [("RW-01","Free Taco",100),("RW-02","$3 off",200)]
    rdf = spark.createDataFrame(rewards, ["RewardCode","RewardName","PointsCost"]) \
               .withColumn("RewardID", F.monotonically_increasing_id()+1) \
               .withColumn("IsActive", F.lit(1)).withColumn("StartDateID", F.lit(20000)).withColumn("CreatedDate", F.current_timestamp())
    write_parquet(rdf.select("RewardID","RewardCode","RewardName","PointsCost","IsActive","StartDateID","CreatedDate"), f"{output_root}/loyalty.Rewards")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    n = int(sys.argv[2]) if len(sys.argv)>2 else 2000
    main(out,n)
