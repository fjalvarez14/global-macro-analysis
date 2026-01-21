import duckdb
import sys

con = duckdb.connect('data/warehouse.duckdb')

print("=" * 60)
print("TABLES IN DATABASE")
print("=" * 60)
print(con.execute('SHOW TABLES').fetchdf())
print()

print("=" * 60)
print("ROW COUNTS")
print("=" * 60)

tables = [
    'raw.country_metadata',
    'raw.wb_indicators', 
    'raw.hdr_indicators',
    'raw.imf_weo_indicators',
    'raw.imf_fdi_indicator'
]

for table in tables:
    try:
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {count:,} rows")
    except:
        print(f"{table}: Not found")

print()

if len(sys.argv) > 1:
    query = ' '.join(sys.argv[1:])
    print("=" * 60)
    print(f"QUERY: {query}")
    print("=" * 60)
    print(con.execute(query).fetchdf())

con.close()
