import sqlite3
import os

# SQLite database path
db_path = os.path.expanduser("~/stock_data.db")

def create_and_populate_table():
    """Create test-myproject-table in SQLite and populate it from 1.csv"""
    try:
        print(f"Connecting to SQLite database: {db_path}")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("✓ Connected successfully")

        # Drop table if exists
        print("\nDropping table if exists...")
        cursor.execute("DROP TABLE IF EXISTS [test-myproject-table]")
        conn.commit()
        print("✓ Old table dropped (if existed)")

        # Create the main table
        print("\nCreating [test-myproject-table]...")
        cursor.execute("""
            CREATE TABLE [test-myproject-table] (
                StockNumber INTEGER PRIMARY KEY,
                Symbol TEXT NOT NULL,
                CompanyName TEXT,
                MarketType TEXT,
                Price REAL,
                Timestamp DATETIME
            )
        """)
        conn.commit()
        print("✓ Table created successfully")

        # Create StockPriceHistory table if it doesn't exist
        print("\nCreating StockPriceHistory table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS StockPriceHistory (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                StockNumber INTEGER,
                Symbol TEXT,
                CompanyName TEXT,
                Price REAL,
                Timestamp DATETIME,
                MarketType TEXT
            )
        """)
        conn.commit()
        print("✓ StockPriceHistory table ready")

        # Read and parse the CSV file
        print("\nReading data from 1.csv...")
        stocks = []
        
        # Try different encodings
        encodings = ['utf-16', 'utf-8', 'latin-1', 'cp1252']
        content = None
        
        for encoding in encodings:
            try:
                with open('1.csv', 'r', encoding=encoding) as f:
                    content = f.read()
                print(f"✓ File read successfully using {encoding} encoding")
                break
            except:
                continue
        
        if content is None:
            raise Exception("Could not read file with any supported encoding")
            
        # Parse the pipe-delimited format
        lines = content.strip().split('\n')
        
        # Skip header and separator lines
        data_lines = [line for line in lines[2:] if line and not line.startswith('Total records')]
        
        for line in data_lines:
            parts = [p.strip() for p in line.split('|')]
            if len(parts) >= 6:
                stock_number = int(parts[0])
                company_name = parts[1]
                price = float(parts[2]) if parts[2] else None
                timestamp = parts[3] if parts[3] else None
                symbol = parts[4]
                market_type = parts[5]
                
                stocks.append((stock_number, symbol, company_name, market_type, price, timestamp))
        
        print(f"✓ Parsed {len(stocks)} stocks from CSV")

        # Insert data into table
        print("\nInserting data into [test-myproject-table]...")
        inserted = 0
        
        for stock in stocks:
            try:
                cursor.execute("""
                    INSERT INTO [test-myproject-table] 
                    (StockNumber, Symbol, CompanyName, MarketType, Price, Timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, stock)
                inserted += 1
            except Exception as e:
                print(f"  ✗ Error inserting stock {stock[0]}: {e}")
        
        conn.commit()
        print(f"✓ Inserted {inserted} records successfully")

        # Verify the data
        print("\nVerifying data...")
        cursor.execute("SELECT COUNT(*) FROM [test-myproject-table]")
        count = cursor.fetchone()[0]
        print(f"✓ Total records in table: {count}")

        # Show sample data
        print("\nSample data (first 5 rows):")
        cursor.execute("""
            SELECT StockNumber, Symbol, CompanyName, MarketType, Price, Timestamp
            FROM [test-myproject-table]
            ORDER BY StockNumber
            LIMIT 5
        """)
        rows = cursor.fetchall()
        for row in rows:
            print(f"  {row[0]}. {row[1]} ({row[3]}) - {row[2]} - Price: {row[4]} - {row[5]}")

        # Show last 5 rows
        print("\nSample data (last 5 rows):")
        cursor.execute("""
            SELECT StockNumber, Symbol, CompanyName, MarketType, Price, Timestamp
            FROM [test-myproject-table]
            ORDER BY StockNumber DESC
            LIMIT 5
        """)
        rows = cursor.fetchall()
        for row in rows:
            print(f"  {row[0]}. {row[1]} ({row[3]}) - {row[2]} - Price: {row[4]} - {row[5]}")

        cursor.close()
        conn.close()
        print("\n✓ Database connection closed")
        print("=" * 70)
        print(f"SUCCESS: Table created and populated successfully!")
        print(f"Database location: {db_path}")
        print("=" * 70)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise

if __name__ == "__main__":
    create_and_populate_table()
