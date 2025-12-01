# 13_dbo_audit_system_error.py
from utils import get_spark, write_parquet
import pyspark.sql.functions as F

def main(output_root="./output_parquet"):
    spark = get_spark("dbo_misc")
    # SystemConfiguration sample
    cfg = [("default.tax.rate","0.07","Default tax rate","Decimal","0","system")]
    cfg_df = spark.createDataFrame(cfg, ["ConfigKey","ConfigValue","Description","DataType","IsEncrypted","ModifiedBy"]) \
              .withColumn("ConfigID", F.monotonically_increasing_id()+1).withColumn("ModifiedDate", F.current_timestamp())
    write_parquet(cfg_df.select("ConfigID","ConfigKey","ConfigValue","Description","DataType","IsEncrypted","ModifiedBy","ModifiedDate"),
                  f"{output_root}/dbo.SystemConfiguration")

    # Empty AuditLog/ErrorLog skeletons
    audit_schema = spark.createDataFrame([], schema="AuditID long, TableName string, RecordID long, Action string, OldValues string, NewValues string, ChangedBy string, ChangedDate timestamp, IPAddress string, ApplicationName string")
    write_parquet(audit_schema, f"{output_root}/dbo.AuditLog")

    error_schema = spark.createDataFrame([], schema="ErrorID long, ErrorNumber int, ErrorSeverity int, ErrorState int, ErrorProcedure string, ErrorLine int, ErrorMessage string, UserName string, HostName string, ApplicationName string, ErrorDate timestamp")
    write_parquet(error_schema, f"{output_root}/dbo.ErrorLog")
    spark.stop()

if __name__=="__main__":
    import sys
    out = sys.argv[1] if len(sys.argv)>1 else "./output_parquet"
    main(out)
