# azure_config.py
"""
Azure Blob Storage Configuration
Replace these values with your actual Azure credentials
"""

# Azure Storage Account Details
STORAGE_ACCOUNT_NAME = "your_storage_account_name"  # Replace with your storage account name
STORAGE_ACCOUNT_KEY = "your_storage_account_key"    # Replace with your storage account key
CONTAINER_NAME = "spicyfiesta-data"                  # Replace with your container name

# Folder structure in blob storage
FOLDERS = {
    "dim": "dimensions",          # dim.Calendar
    "store": "store",            # store.States, store.Locations, store.OperatingHours
    "emp": "employee",           # emp.Positions, emp.Employees, etc.
    "menu": "menu",              # menu.Categories, menu.Items, etc.
    "inv": "inventory",          # inv.Items, inv.StoreInventory, etc.
    "promo": "promotions",       # promo.Promotions, promo.PromotionItems, etc.
    "loyalty": "loyalty",        # loyalty.Members, loyalty.Rewards, etc.
    "ord": "orders",             # ord.Orders, ord.OrderItems, etc.
    "finance": "finance",        # finance.Ledger, finance.DailySalesSummary, etc.
    "dbo": "system"              # dbo.SystemConfiguration, dbo.AuditLog, etc.
}

def get_output_path(schema, table_name):
    """
    Generate the full Azure path for a given schema and table
    
    Args:
        schema: Schema name (e.g., "store", "emp", "ord")
        table_name: Table name (e.g., "States", "Employees", "Orders")
    
    Returns:
        Relative path like "store/States"
    """
    folder = FOLDERS.get(schema, schema)
    return f"{folder}/{table_name}"