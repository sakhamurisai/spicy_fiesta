# azure_config.py

# Azure Storage Account Details
STORAGE_ACCOUNT_NAME = "restaurantdata"
STORAGE_ACCOUNT_KEY = ""
CONTAINER_NAME = "inputblob"

def get_azure_blob_path(schema):
    return f"wasbs://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{schema}"


def configure_azure_blob_storage(spark):
     spark.conf.set(
        f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        STORAGE_ACCOUNT_KEY
    )
