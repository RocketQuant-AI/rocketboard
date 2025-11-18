"""Test script for parquet + DuckDB pipeline."""
import sys
import pandas as pd
from pathlib import Path

# Test 1: Create sample data and save to parquet
print("Test 1: Creating sample price data and saving to parquet...")

sample_data = [
    {
        "date": "2024-01-01",
        "open": 150.0,
        "high": 155.0,
        "low": 149.0,
        "close": 153.0,
        "adjClose": 153.0,
        "volume": 1000000
    },
    {
        "date": "2024-01-02",
        "open": 153.0,
        "high": 157.0,
        "low": 152.0,
        "close": 156.0,
        "adjClose": 156.0,
        "volume": 1200000
    },
]

df = pd.DataFrame(sample_data)
parquet_dir = Path("../data/price/daily_stock_price")
parquet_dir.mkdir(parents=True, exist_ok=True)

test_file = parquet_dir / "test_aapl.parquet"
df.to_parquet(test_file, index=False, engine='pyarrow')
print(f"✓ Saved test data to {test_file}")
print(f"  Rows: {len(df)}")

# Test 2: Read the parquet file
print("\nTest 2: Reading parquet file...")
df_read = pd.read_parquet(test_file)
print(f"✓ Read {len(df_read)} rows from parquet")
print(df_read)

# Test 3: Load into DuckDB
print("\nTest 3: Loading into DuckDB...")
# Add price module to path to import init_duckdb
sys.path.insert(0, str(Path(__file__).parent.parent / "price"))

from init_duckdb import create_price_schema

db_path = "../data/price/price.duckdb"
con = create_price_schema(db_path)

# Insert the test data
con.execute(f"""
    INSERT OR REPLACE INTO fact_price_daily
    SELECT 
        'TEST_AAPL' as ticker,
        CAST(date as DATE) as dt,
        open, high, low, close,
        "adjClose" as adj_close,
        volume
    FROM read_parquet('{test_file}')
""")

print("✓ Data loaded into DuckDB")

# Test 4: Query the data
print("\nTest 4: Querying DuckDB...")
result = con.execute("""
    SELECT * 
    FROM fact_price_daily
    WHERE ticker = 'TEST_AAPL'
    ORDER BY dt
""").fetchdf()

print(f"✓ Retrieved {len(result)} rows from DuckDB")
print(result)

# Test 5: Aggregation query
print("\nTest 5: Running aggregation query...")
stats = con.execute("""
    SELECT 
        ticker,
        COUNT(*) as num_days,
        MIN(dt) as start_date,
        MAX(dt) as end_date,
        AVG(close) as avg_close,
        MAX(high) as max_high,
        MIN(low) as min_low
    FROM fact_price_daily
    WHERE ticker = 'TEST_AAPL'
    GROUP BY ticker
""").fetchdf()

print("✓ Aggregation results:")
print(stats)

con.close()

print("\n" + "="*50)
print("✓ All tests passed!")
print("="*50)
print("\nThe parquet + DuckDB pipeline is working correctly.")
print("You can now use async_history_price.py to fetch and save real data.")

