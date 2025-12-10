import sqlite3
import os

db_path = os.path.expanduser('~/stock_data.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()

print(f'Database: {db_path}')
print(f'\nTotal tables: {len(tables)}')
print('\nTables in your SQLite database:')
for i, table in enumerate(tables, 1):
    table_name = table[0]
    cursor.execute(f'SELECT COUNT(*) FROM [{table_name}]')
    count = cursor.fetchone()[0]
    print(f'  {i}. {table_name} ({count} records)')

conn.close()
