import pypyodbc
import csv
from datetime import datetime
from credential import username, password, server, database

# Connection string
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"

def create_and_populate_table():
    """Create test-myproject-table and populate it from 1.csv"""
    try:
        print("Connecting to SQL Server...")
        conn = pypyodbc.connect(conn_str)
        cursor = conn.cursor()
        print("✓ Connected successfully")

        # Drop table if exists
        print("\nDropping table if exists...")
        cursor.execute("""
            IF OBJECT_ID('[dbo].[test-myproject-table]', 'U') IS NOT NULL
                DROP TABLE [dbo].[test-myproject-table]
        """)
        conn.commit()
        print("✓ Old table dropped (if existed)")

        # Create the main table
        print("\nCreating [dbo].[test-myproject-table]...")
        cursor.execute("""
            CREATE TABLE [dbo].[test-myproject-table] (
                StockNumber INT PRIMARY KEY,
                Symbol NVARCHAR(50) NOT NULL,
                CompanyName NVARCHAR(255),
                MarketType NVARCHAR(10),
                Price DECIMAL(18, 2),
                Timestamp DATETIME
            )
        """)
        conn.commit()
        print("✓ Table created successfully")

        # Create StockPriceHistory table if it doesn't exist
        print("\nCreating StockPriceHistory table...")
        cursor.execute("""
            IF OBJECT_ID('[dbo].[StockPriceHistory]', 'U') IS NULL
            BEGIN
                CREATE TABLE [dbo].[StockPriceHistory] (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    StockNumber INT,
                    Symbol NVARCHAR(50),
                    CompanyName NVARCHAR(255),
                    Price DECIMAL(18, 2),
                    Timestamp DATETIME,
                    MarketType NVARCHAR(10)
                )
            END
        """)
        conn.commit()
        print("✓ StockPriceHistory table ready")

        # Read and parse the CSV file
        print("\nReading data from 1.csv...")
        stocks = []
        
        with open('1.csv', 'r', encoding='utf-8') as f:
            content = f.read()
            
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
        print("\nInserting data into [dbo].[test-myproject-table]...")
        inserted = 0
        
        for stock in stocks:
            try:
                cursor.execute("""
                    INSERT INTO [dbo].[test-myproject-table] 
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
        cursor.execute("SELECT COUNT(*) FROM [dbo].[test-myproject-table]")
        count = cursor.fetchone()[0]
        print(f"✓ Total records in table: {count}")

        # Show sample data
        print("\nSample data (first 5 rows):")
        cursor.execute("""
            SELECT TOP 5 StockNumber, Symbol, CompanyName, MarketType, Price, Timestamp
            FROM [dbo].[test-myproject-table]
            ORDER BY StockNumber
        """)
        rows = cursor.fetchall()
        for row in rows:
            print(f"  {row[0]}. {row[1]} ({row[3]}) - {row[2]} - Price: {row[4]} - {row[5]}")

        cursor.close()
        conn.close()
        print("\n✓ Database connection closed")
        print("=" * 70)
        print("SUCCESS: Table created and populated successfully!")
        print("=" * 70)

    except Exception as e:
        print(f"\n✗ Error: {e}")
        raise

if __name__ == "__main__":
    create_and_populate_table()
