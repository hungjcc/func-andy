# Azure Storage Database Backup Setup

## Overview
Your SQLite database (`stock_data.db`) is now automatically backed up to Azure Blob Storage after each scheduled execution of the function (daily at 21:30 HKT).

## Configuration Details

### Storage Account
- **Account Name**: `funcstock26092`
- **Container**: `stock-data`
- **Blob Name**: `stock_data.db`

### How It Works
1. The timer-triggered function runs daily at 21:30 HKT (13:30 UTC)
2. Stocks are fetched and updated in the SQLite database
3. **Automatically**, the database is uploaded to Azure Blob Storage
4. The blob is overwritten with the latest version each time

### Key Code Changes
- Added `azure-storage-blob` to `requirements.txt`
- Added `upload_db_to_blob()` function in `function_app.py`
- Function is automatically called after stock updates
- Connection uses the `AzureWebJobsStorage` app setting

## Verification

### View Blob in Azure Portal
1. Go to Azure Portal → Storage Account: `funcstock26092`
2. Navigate to: Containers → `stock-data`
3. You'll see `stock_data.db` with details:
   - **Size**: ~16 KB (SQLite database with 6 stocks)
   - **Last Modified**: Updated daily after function runs
   - **Type**: BlockBlob

### Download Backup via Azure CLI
```powershell
az storage blob download `
  --container-name stock-data `
  --name stock_data.db `
  --account-name funcstock26092 `
  --account-key "YOUR_ACCOUNT_KEY" `
  --file backup_stock_data.db
```

### List All Backups
```powershell
az storage blob list `
  --container-name stock-data `
  --account-name funcstock26092 `
  --account-key "YOUR_ACCOUNT_KEY"
```

## Local Testing

To test the blob upload locally:
```python
from azure.storage.blob import BlobClient
import os

conn_str = 'YOUR_CONNECTION_STRING'
db_path = os.path.expanduser('~/stock_data.db')

blob_client = BlobClient.from_connection_string(
    conn_str,
    container_name='stock-data',
    blob_name='stock_data.db'
)

with open(db_path, 'rb') as data:
    blob_client.upload_blob(data, overwrite=True)
```

## Automatic Backup Details

### When Backups Occur
- **Time**: Daily at 21:30 HKT (13:30 UTC)
- **Frequency**: Once per day
- **Duration**: ~1-2 seconds per upload

### What Gets Backed Up
- Complete SQLite database with:
  - `stocks` table (6 sample stocks)
  - `StockPriceHistory` table (historical prices)

### Log Messages
You'll see these logs in Azure Portal when the backup succeeds:
```
✓ Database uploaded to Azure Blob Storage (16384 bytes)
```

## Troubleshooting

### Check Function Logs
1. Azure Portal → Function App: `func-andy2609`
2. Navigate to: Monitor tab
3. Look for log entries mentioning "uploaded to Azure Blob Storage"

### If Upload Fails
- Verify `AzureWebJobsStorage` connection string is set correctly
- Check container `stock-data` exists
- Ensure function app has permissions to the storage account

### Restore from Backup
To restore from a backup:
```powershell
# Download the backup
az storage blob download `
  --container-name stock-data `
  --name stock_data.db `
  --account-name funcstock26092 `
  --file restored_database.db
```

## Cost Considerations
- **Storage**: ~16 KB per backup (negligible cost)
- **Operations**: One upload per day (~1 operation)
- Estimated monthly cost: **$0.00-$0.01** (within free tier for most scenarios)

## Future Enhancements
Consider adding:
1. **Versioned backups** - Keep multiple snapshots (daily, weekly, monthly)
2. **Blob snapshots** - Automatic snapshots at specific intervals
3. **Blob lifecycle** - Archive old backups to cool storage
4. **Download endpoint** - REST API to retrieve backups on demand

---

**Deployment Date**: December 10, 2025
**Function App**: func-andy2609
**Region**: East Asia
