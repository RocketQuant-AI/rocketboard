"""Load parquet files into DuckDB."""
import duckdb
from pathlib import Path
from tqdm import tqdm
from init_duckdb import create_price_schema


def load_parquet_to_duckdb(
    parquet_dir: str = "../data/price/daily_stock_price",
    db_path: str = "../data/price/price.duckdb",
    initialize: bool = True
):
    """
    Load all parquet files into DuckDB.
    
    Args:
        parquet_dir: Directory containing parquet files (one per ticker)
        db_path: Path to DuckDB database file
        initialize: Whether to initialize schema if not exists
        
    Returns:
        DuckDB connection object
    """
    # Initialize schema if needed
    if initialize:
        con = create_price_schema(db_path)
    else:
        con = duckdb.connect(db_path)
    
    # Find all parquet files
    parquet_path = Path(parquet_dir)
    if not parquet_path.exists():
        print(f"Error: Directory {parquet_dir} does not exist")
        return con
    
    parquet_files = list(parquet_path.glob("*.parquet"))
    
    if not parquet_files:
        print(f"No parquet files found in {parquet_dir}")
        return con
    
    print(f"\nLoading {len(parquet_files)} parquet files into DuckDB...")
    
    success_count = 0
    error_count = 0
    
    for pq_file in tqdm(parquet_files, desc="Loading tickers"):
        ticker = pq_file.stem.upper()
        
        try:
            # Insert or replace data for this ticker
            con.execute(f"""
                INSERT OR REPLACE INTO fact_price_daily
                SELECT 
                    '{ticker}' as ticker,
                    CAST(date as DATE) as dt,
                    open, high, low, close,
                    "adjClose" as adj_close,
                    volume
                FROM read_parquet('{pq_file}')
            """)
            success_count += 1
            
        except Exception as e:
            print(f"\n✗ Error loading {ticker}: {e}")
            error_count += 1
    
    print(f"\n✓ Successfully loaded: {success_count} tickers")
    if error_count > 0:
        print(f"✗ Errors: {error_count} tickers")
    
    # Show summary statistics
    print("\nDatabase summary:")
    result = con.execute("""
        SELECT 
            COUNT(DISTINCT ticker) as num_tickers,
            COUNT(*) as total_rows,
            MIN(dt) as earliest_date,
            MAX(dt) as latest_date
        FROM fact_price_daily
    """).fetchone()
    
    print(f"  Tickers: {result[0]}")
    print(f"  Total rows: {result[1]:,}")
    print(f"  Date range: {result[2]} to {result[3]}")
    
    return con


def query_ticker(ticker: str, db_path: str = "../data/price/price.duckdb", limit: int = 10):
    """
    Query price data for a specific ticker.
    
    Args:
        ticker: Stock ticker symbol
        db_path: Path to DuckDB database file
        limit: Number of recent rows to return
    """
    con = duckdb.connect(db_path)
    
    result = con.execute(f"""
        SELECT * 
        FROM fact_price_daily
        WHERE ticker = '{ticker.upper()}'
        ORDER BY dt DESC
        LIMIT {limit}
    """).fetchdf()
    
    con.close()
    return result


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Load parquet files into DuckDB")
    parser.add_argument("--parquet-dir", default="../data/price/daily_stock_price",
                       help="Directory containing parquet files")
    parser.add_argument("--db-path", default="../data/price/price.duckdb",
                       help="Path to DuckDB database file")
    parser.add_argument("--query", type=str, help="Query a specific ticker after loading")
    
    args = parser.parse_args()
    
    # Load data
    con = load_parquet_to_duckdb(args.parquet_dir, args.db_path)
    
    # Optional: query a ticker
    if args.query:
        print(f"\nQuerying {args.query}...")
        df = query_ticker(args.query, args.db_path)
        print(df)
    
    con.close()

