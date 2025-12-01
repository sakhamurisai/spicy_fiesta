states = [
    ("AL","Alabama","South",0.04),
    ("AK","Alaska","West",0.00),  # no state sales tax
    ("AZ","Arizona","West",0.056),
    ("AR","Arkansas","South",0.065),
    ("CA","California","West",0.0725),
    ("CO","Colorado","West",0.029),
    ("CT","Connecticut","Northeast",0.0635),
    ("DE","Delaware","Northeast",0.00),  # no state sales tax
    ("FL","Florida","South",0.06),
    ("GA","Georgia","South",0.04),
    ("HI","Hawaii","West",0.04),
    ("ID","Idaho","West",0.06),
    ("IL","Illinois","Midwest",0.0625),
    ("IN","Indiana","Midwest",0.07),
    ("IA","Iowa","Midwest",0.06),
    ("KS","Kansas","Midwest",0.065),
    ("KY","Kentucky","South",0.06),
    ("LA","Louisiana","South",0.0445),
    ("ME","Maine","Northeast",0.055),
    ("MD","Maryland","South",0.06),
    ("MA","Massachusetts","Northeast",0.0625),
    ("MI","Michigan","Midwest",0.06),
    ("MN","Minnesota","Midwest",0.06875),
    ("MS","Mississippi","South",0.07),
    ("MO","Missouri","Midwest",0.04225),
    ("MT","Montana","West",0.00),  # no state sales tax
    ("NE","Nebraska","Midwest",0.055),
    ("NV","Nevada","West",0.0685),
    ("NH","New Hampshire","Northeast",0.00),  # no state sales tax
    ("NJ","New Jersey","Northeast",0.06625),
    ("NM","New Mexico","West",0.05125),
    ("NY","New York","Northeast",0.04),
    ("NC","North Carolina","South",0.0475),
    ("ND","North Dakota","Midwest",0.05),
    ("OH","Ohio","Midwest",0.0575),
    ("OK","Oklahoma","South",0.045),
    ("OR","Oregon","West",0.00),  # no state sales tax
    ("PA","Pennsylvania","Northeast",0.06),
    ("RI","Rhode Island","Northeast",0.07),
    ("SC","South Carolina","South",0.06),
    ("SD","South Dakota","Midwest",0.045),
    ("TN","Tennessee","South",0.07),
    ("TX","Texas","South",0.0625),
    ("UT","Utah","West",0.0485),
    ("VT","Vermont","Northeast",0.06),
    ("VA","Virginia","South",0.053),
    ("WA","Washington","West",0.065),
    ("WV","West Virginia","South",0.06),
    ("WI","Wisconsin","Midwest",0.05),
    ("WY","Wyoming","West",0.04),
    ("DC","District of Columbia","South",0.06)
]

# 01_store_states.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("states")
    states = [
        ("CA","California","West",0.0725),
        ("TX","Texas","South",0.0625),
        ("NY","New York","Northeast",0.04),
        ("FL","Florida","South",0.06),
        ("IL","Illinois","Midwest",0.0625)
    ]
    df = spark.createDataFrame(states, ["StateCode","StateName","StateRegion","TaxRate"]) \
              .withColumn("IsActive", F.lit(1)) \
              .withColumn("CreatedDate", F.current_timestamp()) \
              .withColumn("ModifiedDate", F.current_timestamp())
    # add StateID
    from pyspark.sql.functions import monotonically_increasing_id
    df = df.withColumn("StateID", (monotonically_increasing_id()+1).cast("int")) \
           .select("StateID","StateCode","StateName","StateRegion","TaxRate","IsActive","CreatedDate","ModifiedDate")
    write_parquet(df, f"{output_root}/store.States")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
