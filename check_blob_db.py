#!/usr/bin/env python
"""Check what tables exist in the blob database"""

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
        
        print(f"✓ Downloaded to {temp_file.name}")
        return temp_file.name
    except Exception as e:
        print(f"✗ Error: {e}")
        return None

def check_tables(db_path):
    """List all tables in the database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        print("\n📋 Tables in database:")
        if tables:
            for table in tables:
                print(f"  - {table[0]}")
                
                # Get column info
                cursor.execute(f"PRAGMA table_info({table[0]})")
                columns = cursor.fetchall()
                for col in columns:
                    print(f"      • {col[1]} ({col[2]})")
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
                count = cursor.fetchone()[0]
                print(f"      Rows: {count}")
        else:
            print("  ⚠️  No tables found!")
        
        conn.close()
    except Exception as e:
        print(f"✗ Error checking tables: {e}")

def main():
    print("=" * 60)
    print("Checking Blob Database Structure")
    print("=" * 60)
    
    db_path = download_db_from_blob()
    if db_path:
        check_tables(db_path)
        if os.path.exists(db_path):
            os.remove(db_path)
            print("\n🗑️  Cleaned up temporary file")

if __name__ == "__main__":
    main()
