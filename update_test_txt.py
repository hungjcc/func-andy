#!/usr/bin/env python
"""Download stock_data.db from Azure Blob Storage and display in test.txt"""

import os
import sqlite3
from io import BytesIO
import tempfile
from azure.storage.blob import BlobClient

# Configuration
STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
BLOB_CONTAINER_NAME = "stock-data"
BLOB_NAME = "stock_data.db"

def download_db_from_blob():
    """Download the SQLite database from Azure Blob Storage to a temporary file."""
    try:
        if not STORAGE_CONNECTION_STRING:
            print("⚠️  AzureWebJobsStorage not configured.")
            return None
        
        blob_client = BlobClient.from_connection_string(
            STORAGE_CONNECTION_STRING,
            container_name=BLOB_CONTAINER_NAME,
            blob_name=BLOB_NAME
        )
        
        download_stream = blob_client.download_blob()
        db_bytes = BytesIO(download_stream.readall())
        
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_file.write(db_bytes.getvalue())
        temp_file.close()
        
        print(f"✓ Downloaded stock_data.db from Azure Blob Storage")
        return temp_file.name
    except Exception as e:
        print(f"✗ Error downloading database: {e}")
        return None

def export_to_text(db_path, output_file):
    """Export stock data from database to text file"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Query all stocks from the stocks table
        cursor.execute("""
            SELECT StockNumber, CompanyName, Price, Timestamp, Symbol, MarketType
            FROM stocks
            ORDER BY MarketType, Symbol
        """)
        
        rows = cursor.fetchall()
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("StockNumber\tCompanyName\t\tPrice\t\tTimestamp\t\t\tSymbol\tMarket\n")
            f.write("=" * 120 + "\n")
            
            for idx, (stock_num, name, price, timestamp, symbol, market) in enumerate(rows, 1):
                # Format the line
                line = f"{idx}\t{name[:20]:<20}\t{price}\t\t{timestamp}\t{symbol}\t{market}\n"
                f.write(line)
        
        conn.close()
        print(f"✓ Exported {len(rows)} stocks to {output_file}")
        return True
    except Exception as e:
        print(f"✗ Error exporting to text: {e}")
        return False

def main():
    print("=" * 60)
    print("Updating test.txt with Latest Stock Data")
    print("=" * 60)
    
    db_path = download_db_from_blob()
    if db_path:
        output_file = "test.txt"
        export_to_text(db_path, output_file)
        
        # Clean up
        if os.path.exists(db_path):
            os.remove(db_path)
            print("✓ Cleaned up temporary files")
        
        # Display the file
        print("\n📄 Current stock_data.db contents:")
        print("=" * 60)
        with open(output_file, 'r', encoding='utf-8') as f:
            print(f.read())
    else:
        print("✗ Failed to download database")

if __name__ == "__main__":
    main()
