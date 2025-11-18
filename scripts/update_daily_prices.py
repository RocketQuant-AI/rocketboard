#!/usr/bin/env python3
"""
Update daily stock price data.

This script:
1. Fetches historical price data for all tickers and saves as parquet
2. Loads the parquet files into DuckDB for efficient querying

Usage:
    python update_daily_prices.py [--fetch-only] [--load-only] [--start-date YYYY-MM-DD]
"""
import sys
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def fetch_price_data(start_date: str = None):
    """
    Fetch historical price data using async_history_price.py
    
    Args:
        start_date: Optional start date in YYYY-MM-DD format
    """
    print("\n" + "="*60)
    print("STEP 1: Fetching historical price data")
    print("="*60)
    
    price_script = project_root / "price" / "async_history_price.py"
    
    if not price_script.exists():
        print(f"❌ Error: {price_script} not found")
        return False
    
    try:
        # Note: The async_history_price.py uses hardcoded start_date
        # If you want to pass start_date as argument, you'll need to modify that file
        print(f"Running: python {price_script}")
        
        result = subprocess.run(
            [sys.executable, str(price_script)],
            cwd=price_script.parent,
            capture_output=False,  # Show output in real-time
            text=True
        )
        
        if result.returncode == 0:
            print("\n✓ Price data fetch completed successfully")
            return True
        else:
            print(f"\n❌ Price data fetch failed with code {result.returncode}")
            return False
            
    except Exception as e:
        print(f"❌ Error fetching price data: {e}")
        return False


def load_to_duckdb():
    """Load parquet files into DuckDB."""
    print("\n" + "="*60)
    print("STEP 2: Loading parquet files into DuckDB")
    print("="*60)
    
    try:
        # Import the load function
        sys.path.insert(0, str(project_root / "price"))
        from load_to_duckdb import load_parquet_to_duckdb
        
        # Load data
        con = load_parquet_to_duckdb(
            parquet_dir=str(project_root / "data" / "price" / "daily_stock_price"),
            db_path=str(project_root / "data" / "price" / "price.duckdb"),
            initialize=True
        )
        
        if con:
            con.close()
            print("\n✓ Data loaded into DuckDB successfully")
            return True
        else:
            print("\n❌ Failed to load data into DuckDB")
            return False
            
    except Exception as e:
        print(f"❌ Error loading to DuckDB: {e}")
        import traceback
        traceback.print_exc()
        return False


def show_summary():
    """Show summary statistics from DuckDB."""
    print("\n" + "="*60)
    print("SUMMARY: Database Statistics")
    print("="*60)
    
    try:
        import duckdb
        
        db_path = project_root / "data" / "price" / "price.duckdb"
        if not db_path.exists():
            print("❌ Database not found")
            return
        
        con = duckdb.connect(str(db_path))
        
        # Overall statistics
        stats = con.execute("""
            SELECT 
                COUNT(DISTINCT ticker) as total_tickers,
                COUNT(*) as total_rows,
                MIN(dt) as earliest_date,
                MAX(dt) as latest_date
            FROM fact_price_daily
        """).fetchone()
        
        if stats and stats[0] > 0:
            print(f"\n  Total Tickers: {stats[0]:,}")
            print(f"  Total Records: {stats[1]:,}")
            print(f"  Date Range: {stats[2]} to {stats[3]}")
            
            # Sample of recent updates
            print("\n  Recent Data (sample):")
            recent = con.execute("""
                SELECT ticker, MAX(dt) as latest_date, COUNT(*) as days
                FROM fact_price_daily
                GROUP BY ticker
                ORDER BY latest_date DESC
                LIMIT 5
            """).fetchdf()
            print(recent.to_string(index=False))
        else:
            print("\n  No data found in database")
        
        con.close()
        
    except Exception as e:
        print(f"❌ Error fetching summary: {e}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update daily stock price data",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="Only fetch price data, don't load to DuckDB"
    )
    parser.add_argument(
        "--load-only",
        action="store_true",
        help="Only load existing parquet files to DuckDB"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for fetching (YYYY-MM-DD format)"
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("Daily Stock Price Update")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    success = True
    
    # Step 1: Fetch data
    if not args.load_only:
        success = fetch_price_data(args.start_date)
        if not success and not args.fetch_only:
            print("\n❌ Stopping due to fetch error")
            return 1
    
    # Step 2: Load to DuckDB
    if not args.fetch_only:
        success = load_to_duckdb()
        if not success:
            print("\n❌ Stopping due to load error")
            return 1
    
    # Step 3: Show summary
    if not args.fetch_only:
        show_summary()
    
    # Final status
    print("\n" + "="*60)
    if success:
        print("✓ Update completed successfully!")
    else:
        print("❌ Update completed with errors")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())

