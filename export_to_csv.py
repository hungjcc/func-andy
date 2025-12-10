#!/usr/bin/env python
"""Export azure_stock_data.db to CSV file"""

import sqlite3
import csv
import os

# Database path
db_path = "E:\\Andy Cheng\\func-andy\\azure_stock_data.db"
csv_output = "E:\\Andy Cheng\\func-andy\\stock_data.csv"

def export_to_csv():
    """Export stock data from database to CSV"""
    try:
        if not os.path.exists(db_path):
            print(f"✗ Database file not found: {db_path}")
            return False
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Query all stocks from the stocks table
        cursor.execute("""
            SELECT StockNumber, Symbol, CompanyName, MarketType, Price, Timestamp
            FROM stocks
            ORDER BY MarketType, Symbol
        """)
        
        rows = cursor.fetchall()
        
        # Write to CSV
        with open(csv_output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # Write header
            writer.writerow(['StockNumber', 'Symbol', 'CompanyName', 'MarketType', 'Price', 'Timestamp'])
            # Write data rows
            writer.writerows(rows)
        
        conn.close()
        print(f"✓ Exported {len(rows)} stocks to CSV")
        print(f"📁 File location: {csv_output}")
        return True
    except Exception as e:
        print(f"✗ Error exporting to CSV: {e}")
        return False

def display_csv():
    """Display the CSV file content"""
    try:
        with open(csv_output, 'r', encoding='utf-8') as f:
            content = f.read()
        
        print("\n" + "=" * 100)
        print("CSV File Contents:")
        print("=" * 100)
        print(content)
        print("=" * 100)
    except Exception as e:
        print(f"✗ Error reading CSV: {e}")

def main():
    print("=" * 100)
    print("Exporting Database to CSV")
    print("=" * 100)
    
    if export_to_csv():
        display_csv()
        print("\n✅ Export completed successfully!")
    else:
        print("\n✗ Export failed!")

if __name__ == "__main__":
    main()
