import sqlite3
import os

db_path = os.path.expanduser("~/stock_data.db")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Insert sample stocks
sample_stocks = [
    (1, '0700', 'Tencent Holdings', 'HK', None, None),
    (2, '3690', 'AIA Group', 'HK', None, None),
    (3, '1398', 'Industrial and Commercial Bank of China', 'HK', None, None),
    (4, 'AAPL', 'Apple Inc', 'US', None, None),
    (5, 'MSFT', 'Microsoft Corporation', 'US', None, None),
    (6, 'GOOGL', 'Alphabet Inc', 'US', None, None),
]

cursor.executemany("""
    INSERT OR IGNORE INTO stocks (StockNumber, Symbol, CompanyName, MarketType, Price, Timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
""", sample_stocks)

conn.commit()

# Show what was inserted
cursor.execute("SELECT * FROM stocks")
rows = cursor.fetchall()

print(f"✓ Inserted/verified {len(rows)} stocks:\n")
for row in rows:
    print(f"  {row[1]} ({row[3]}) - {row[2]}")

conn.close()
