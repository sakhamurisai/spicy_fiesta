"""System configuration, audit log, and error log generation module."""
from utils import get_spark, write_parquet
from azure_config import *
import pyspark.sql.functions as F
import logging

logger = logging.getLogger(__name__)


def main():
    """Generate system tables."""
    spark = get_spark("dbo_misc")
    configure_azure_blob_storage(spark)
    
    try:
        azure_path = get_azure_blob_path("dbo")
        
        # SystemConfiguration
        cfg = [("default.tax.rate","0.07","Default tax rate","Decimal","0","system")]
        cfg_df = spark.createDataFrame(cfg, ["ConfigKey","ConfigValue","Description","DataType","IsEncrypted","ModifiedBy"]) \
                  .withColumn("ConfigID", F.monotonically_increasing_id()+1) \
                  .withColumn("ModifiedDate", F.current_timestamp())
        write_parquet(cfg_df.select("ConfigID","ConfigKey","ConfigValue","Description","DataType","IsEncrypted","ModifiedBy","ModifiedDate"),
                      azure_path)
        
        # Empty AuditLog skeleton
        audit_schema = spark.createDataFrame([], schema="AuditID long, TableName string, RecordID long, Action string, OldValues string, NewValues string, ChangedBy string, ChangedDate timestamp, IPAddress string, ApplicationName string")
        write_parquet(audit_schema, azure_path)
        
        # Empty ErrorLog skeleton
        error_schema = spark.createDataFrame([], schema="ErrorID long, ErrorNumber int, ErrorSeverity int, ErrorState int, ErrorProcedure string, ErrorLine int, ErrorMessage string, UserName string, HostName string, ApplicationName string, ErrorDate timestamp")
        write_parquet(error_schema, azure_path)
        
        logger.info("System tables created successfully")
        
    except Exception as e:
        logger.error(f"Error generating system tables: {str(e)}")
        raise
if __name__ == "__main__":
    main()