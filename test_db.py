import os
import sqlite3

# Check database path
home = os.path.expanduser("~")
db_path = os.path.join(home, "stock_data.db")

if os.path.exists("/tmp"):
    db_path = "/tmp/stock_data.db"

print(f"Database will be created at: {db_path}")
print(f"Home directory: {home}")
print(f"/tmp exists: {os.path.exists('/tmp')}")

# Create the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stocks (
        StockNumber INTEGER PRIMARY KEY,
        Symbol TEXT NOT NULL,
        CompanyName TEXT,
        MarketType TEXT,
        Price REAL,
        Timestamp DATETIME
    )
""")

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
conn.close()

print(f"\n✓ Database created successfully at: {db_path}")
print(f"✓ File exists: {os.path.exists(db_path)}")
print(f"✓ File size: {os.path.getsize(db_path)} bytes")
