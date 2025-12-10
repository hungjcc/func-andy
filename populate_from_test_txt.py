#!/usr/bin/env python
"""Read test.txt raw data and populate it to stock_data.db in Azure Blob Storage"""

import os
import sqlite3
from io import BytesIO
import tempfile
from datetime import datetime
from azure.storage.blob import BlobClient

# Configuration
STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
BLOB_CONTAINER_NAME = "stock-data"
BLOB_NAME = "stock_data.db"
INPUT_FILE = "test.txt"

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

def upload_db_to_blob(db_path):
    """Upload the SQLite database to Azure Blob Storage."""
    try:
        if not STORAGE_CONNECTION_STRING:
            print("⚠️  AzureWebJobsStorage not configured. Skipping upload.")
            return False
        
        if not os.path.exists(db_path):
            print(f"⚠️  Database file not found at {db_path}.")
            return False
        
        blob_client = BlobClient.from_connection_string(
            STORAGE_CONNECTION_STRING,
            container_name=BLOB_CONTAINER_NAME,
            blob_name=BLOB_NAME
        )
        
        with open(db_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        
        file_size = os.path.getsize(db_path)
        print(f"✓ Database uploaded to Azure Blob Storage ({file_size} bytes)")
        return True
    except Exception as e:
        print(f"✗ Error uploading database: {e}")
        return False

def populate_from_text(db_path, input_file):
    """Read test.txt and populate stock_data.db"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Read input file
        if not os.path.exists(input_file):
            print(f"⚠️  Input file not found: {input_file}")
            return False
        
        # Clear existing stocks table
        cursor.execute("DELETE FROM stocks")
        print(f"✓ Cleared existing stocks from database")
        
        # Parse and insert data
        with open(input_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        inserted = 0
        failed = 0
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue
            
            try:
                # Parse tab-separated values
                parts = line.split('\t')
                if len(parts) < 6:
                    print(f"⚠️  Line {line_num}: Invalid format (expected 6 columns, got {len(parts)})")
                    failed += 1
                    continue
                
                idx, company_name, price, timestamp, symbol, market = parts[:6]
                
                # Convert price to float
                try:
                    price = float(price)
                except ValueError:
                    print(f"⚠️  Line {line_num}: Invalid price '{price}'")
                    failed += 1
                    continue
                
                # Extract stock number from symbol
                # For HK stocks (e.g., "0005"), extract the numeric part
                # For US stocks, use line number as stock number
                if market == "HK":
                    try:
                        stock_number = int(symbol)
                    except ValueError:
                        print(f"⚠️  Line {line_num}: Cannot convert HK symbol '{symbol}' to integer")
                        failed += 1
                        continue
                else:
                    # For US stocks, use line number as unique integer ID
                    stock_number = line_num
                
                # Insert into database
                cursor.execute("""
                    INSERT INTO stocks (StockNumber, Symbol, CompanyName, MarketType, Price, Timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (stock_number, symbol, company_name, market, price, timestamp))
                
                inserted += 1
                print(f"  ✓ Line {line_num}: {symbol} ({market}) - {company_name}: ${price}")
            
            except Exception as e:
                print(f"  ✗ Line {line_num}: Error - {e}")
                failed += 1
        
        # Commit changes
        conn.commit()
        conn.close()
        
        print(f"\n📊 Import Summary: {inserted} inserted, {failed} failed")
        return inserted > 0
    
    except Exception as e:
        print(f"✗ Error populating database: {e}")
        return False

def main():
    print("=" * 70)
    print("Populating stock_data.db from test.txt")
    print("=" * 70)
    
    db_path = download_db_from_blob()
    if not db_path:
        print("✗ Failed to download database from blob storage")
        return
    
    print(f"\n📝 Reading from {INPUT_FILE}...\n")
    
    if populate_from_text(db_path, INPUT_FILE):
        print(f"\n📤 Uploading updated database back to Azure Blob Storage...")
        if upload_db_to_blob(db_path):
            print("\n✅ Successfully updated stock_data.db with test.txt data!")
        else:
            print("\n⚠️  Failed to upload database to blob storage")
    else:
        print("\n✗ Failed to populate database")
    
    # Clean up
    if db_path and os.path.exists(db_path):
        os.remove(db_path)
        print("🗑️  Cleaned up temporary files")

if __name__ == "__main__":
    main()
