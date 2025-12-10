# azure_config.py

# Azure Storage Account Details
STORAGE_ACCOUNT_NAME = "your_storage_account_name"
STORAGE_ACCOUNT_KEY = "your_storage_account_key"
CONTAINER_NAME = "spicyfiesta-data"

# Schema to Folder Mapping
SCHEMA_FOLDERS = {
    "dim": "dimensions",
    "store": "store",
    "emp": "employee",
    "menu": "menu",
    "inv": "inventory",
    "promo": "promotions",
    "loyalty": "loyalty",
    "ord": "orders",
    "finance": "finance",
    "dbo": "system"
}


def get_azure_blob_path(schema):
    """
    Get Azure Blob Storage path for a schema folder.
    
    Args:
        schema: Schema name (e.g., "store", "emp", "dim")
    
    Returns:
        Full Azure Blob Storage wasbs:// path to schema folder
    """
    folder = SCHEMA_FOLDERS.get(schema, schema)
    return f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{folder}"


def configure_azure_blob_storage(spark):
    """
    Configure Spark session with Azure Blob Storage credentials.
    
    Args:
        spark: SparkSession instance
    """
    spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        STORAGE_ACCOUNT_KEY
    )
