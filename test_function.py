#!/usr/bin/env python
"""Test script to manually trigger the stock update function"""

import sys
import os
import sqlite3
from datetime import datetime
from io import BytesIO
import tempfile

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import function components
import yfinance as yf
from azure.storage.blob import BlobClient

# Configuration
STORAGE_CONNECTION_STRING = os.getenv("AzureWebJobsStorage")
BLOB_CONTAINER_NAME = "stock-data"
BLOB_NAME = "stock_data.db"

def download_db_from_blob():
    """Download the SQLite database from Azure Blob Storage to a temporary file."""
    try:
        if not STORAGE_CONNECTION_STRING:
            print("⚠️  AzureWebJobsStorage not configured. Cannot download blob.")
            return None
        
        blob_client = BlobClient.from_connection_string(
            STORAGE_CONNECTION_STRING,
            container_name=BLOB_CONTAINER_NAME,
            blob_name=BLOB_NAME
        )
        
        # Download blob to BytesIO
        download_stream = blob_client.download_blob()
        db_bytes = BytesIO(download_stream.readall())
        
        # Create a temporary file
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
        temp_file.write(db_bytes.getvalue())
        temp_file.close()
        
        print(f"✓ Database downloaded from Azure Blob Storage to {temp_file.name}")
        return temp_file.name
    except Exception as e:
        print(f"✗ Error downloading database from blob storage: {e}")
        return None

def upload_db_to_blob(db_path):
    """Upload the SQLite database to Azure Blob Storage."""
    try:
        if not STORAGE_CONNECTION_STRING:
            print("⚠️  AzureWebJobsStorage not configured. Skipping blob upload.")
            return False
        
        if not os.path.exists(db_path):
            print(f"⚠️  Database file not found at {db_path}. Skipping upload.")
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
        print(f"✗ Error uploading database to blob storage: {e}")
        return False

def get_latest_price_and_name(symbol, market_type):
    query_symbol = f"{symbol}.HK" if market_type == "HK" else symbol
    try:
        ticker = yf.Ticker(query_symbol)
        data = ticker.history(period="1d")
        info = ticker.info
        company_name = info.get('shortName', '')
        if not data.empty:
            price = round(data['Close'].iloc[-1], 2)
            return price, company_name
        else:
            return None, company_name
    except Exception as e:
        print(f"✗ Error fetching from Yahoo Finance for {query_symbol}: {e}")
        return None, None

def update_all(db_path, market_type=None):
    """Update all stocks in the blob database."""
    try:
        print(f"\n📊 Connecting to database at {db_path}...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        if market_type:
            print(f"Fetching stocks for market: {market_type}")
            cursor.execute("""
                SELECT StockNumber, Symbol, CompanyName, MarketType
                FROM stocks
                WHERE MarketType = ?
            """, (market_type,))
        else:
            print("Fetching stocks for all markets")
            cursor.execute("""
                SELECT StockNumber, Symbol, CompanyName, MarketType
                FROM stocks
            """)

        rows = cursor.fetchall()
        total = len(rows)
        if total == 0:
            print("⚠️  No stocks found to update.")
            return

        print(f"\n🔄 Updating {total} stocks (market={market_type or 'ALL'})...\n")
        updated = 0
        failed = 0

        for stock_id, symbol, existing_name, mkt in rows:
            try:
                price, company_name = get_latest_price_and_name(symbol, mkt)
                if price is None:
                    print(f"  ✗ No data for {symbol} ({mkt})")
                    failed += 1
                    continue

                ts = datetime.now()

                # Insert into history table
                cursor.execute("""
                    INSERT INTO StockPriceHistory (StockNumber, Symbol, CompanyName, Price, Timestamp, MarketType)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (stock_id, symbol, company_name, price, ts, mkt))

                # Update main table with latest price and name
                cursor.execute("""
                    UPDATE stocks
                    SET Price = ?, Timestamp = ?, CompanyName = ?
                    WHERE StockNumber = ?
                """, (price, ts, company_name, stock_id))

                conn.commit()
                updated += 1
                print(f"  ✓ {symbol} ({mkt}): ${price}")
            except Exception as e:
                print(f"  ✗ Error processing {symbol} ({mkt}): {e}")
                conn.rollback()
                failed += 1

        print(f"\n📈 Results: Updated {updated}, Failed {failed}, Total {total}")

    except Exception as e:
        print(f"✗ Database error: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass

def main():
    print("=" * 60)
    print("Testing Stock Update Function")
    print("=" * 60)
    
    db_path = None
    try:
        # Download database from blob storage
        db_path = download_db_from_blob()
        if not db_path:
            print("\n✗ Failed to download database from blob storage")
            return
        
        # Update all stocks
        update_all(db_path)
        
        # Upload updated database back to blob storage
        print("\n📤 Uploading updated database back to blob storage...")
        upload_db_to_blob(db_path)
        
        print("\n✅ Test completed successfully!")
    except Exception as e:
        print(f"\n✗ Error during test: {str(e)}")
    finally:
        # Clean up temporary file
        if db_path and os.path.exists(db_path):
            try:
                os.remove(db_path)
                print(f"🗑️  Cleaned up temporary file")
            except Exception as e:
                print(f"⚠️  Failed to clean up: {e}")

if __name__ == "__main__":
    main()
