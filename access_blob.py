from azure.storage.blob import ContainerClient
from datetime import datetime
import os

# Get connection string from environment variable
conn_str = os.getenv('AzureWebJobsStorage')

if not conn_str:
    print("✗ Error: AzureWebJobsStorage environment variable not set")
    print("Please set your Azure Storage connection string in the environment")
    exit(1)

print("Accessing stock-data container in Azure Storage...")
print("=" * 70)

try:
    container = ContainerClient.from_connection_string(conn_str, container_name='stock-data')
    
    print("\n✓ Connected to stock-data container")
    print("\nBlobs in container:")
    
    for blob in container.list_blobs():
        print(f"\n  📦 Blob Name: {blob.name}")
        print(f"     Size: {blob.size:,} bytes")
        print(f"     Last Modified: {blob.last_modified}")
        print(f"     Storage Tier: {blob.blob_tier}")
    
    # Download the database
    print("\n" + "=" * 70)
    print("Downloading stock_data.db from Azure...")
    
    blob_client = container.get_blob_client("stock_data.db")
    
    with open("azure_stock_data.db", "wb") as f:
        download_stream = blob_client.download_blob()
        f.write(download_stream.readall())
    
    import os
    file_size = os.path.getsize("azure_stock_data.db")
    print(f"✓ Downloaded successfully ({file_size:,} bytes)")
    print(f"  Location: {os.path.abspath('azure_stock_data.db')}")
    
except Exception as e:
    print(f"\n✗ Error: {e}")
    import traceback
    traceback.print_exc()
