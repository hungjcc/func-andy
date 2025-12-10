import sqlite3
import os

db_path = os.path.expanduser('~/stock_data.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print("Dropping unnecessary tables...")

# Drop the old stocks table
cursor.execute('DROP TABLE IF EXISTS stocks')
conn.commit()
print('✓ Dropped stocks table')

# Check remaining tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cursor.fetchall()

print(f'\nRemaining tables: {len(tables)}')
for table in tables:
    table_name = table[0]
    cursor.execute(f'SELECT COUNT(*) FROM [{table_name}]')
    count = cursor.fetchone()[0]
    print(f'  - {table_name} ({count} records)')

conn.close()
print('\n✓ Cleanup complete!')
