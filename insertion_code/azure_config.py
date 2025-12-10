# azure_config.py
"""
Azure Blob Storage Configuration and Write Function
"""

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


def get_azure_blob_path(table_path):
    """
    Convert local table path to Azure Blob Storage path.
    
    Args:
        table_path: Path in format "schema.TableName" (e.g., "store.States")
    
    Returns:
        Full Azure Blob Storage wasbs:// path
    """
    # Extract schema and table name
    parts = table_path.split('.')
    if len(parts) != 2:
        raise ValueError(f"Invalid table path format: {table_path}. Expected 'schema.TableName'")
    
    schema, table_name = parts
    folder = SCHEMA_FOLDERS.get(schema, schema)
    
    # Construct Azure Blob Storage path
    return f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{folder}/{table_name}"


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
