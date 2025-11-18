"""
Simple example to query stock price data from DuckDB.
"""
import duckdb
import pandas as pd
from pathlib import Path


def get_recent_prices(ticker: str, days: int = 10, db_path: str = None):
    """
    Get recent N days of price data for a given ticker.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        days: Number of recent trading days to retrieve
        db_path: Path to DuckDB database (default: ../data/price/price.duckdb)
    
    Returns:
        pandas.DataFrame with price data
    """
    if db_path is None:
        # Default path relative to this file
        db_path = Path(__file__).parent.parent / "data" / "price" / "price.duckdb"
    
    # Connect to DuckDB (read-only mode)
    con = duckdb.connect(database=str(db_path), read_only=True)
    
    try:
        # Ticker is stored in lowercase in the database
        query = """
        SELECT 
            ticker,
            dt as date,
            open,
            high,
            low,
            close,
            adj_close,
            volume
        FROM fact_price_daily
        WHERE UPPER(ticker) = UPPER(?)
        ORDER BY dt DESC
        LIMIT ?
        """
        
        result_df = con.execute(query, [ticker, days]).fetchdf()
        
        if result_df.empty:
            print(f"No data found for ticker: {ticker}")
            return result_df
        
        # Reverse to show oldest to newest
        result_df = result_df.sort_values('date', ascending=False).reset_index(drop=True)
        
        return result_df
        
    finally:
        con.close()


def print_price_summary(df: pd.DataFrame, ticker: str):
    """Pretty print price data summary."""
    if df.empty:
        return
    
    print(f"\n{'='*70}")
    print(f"Price Data for {ticker.upper()}")
    print(f"{'='*70}")
    print(f"\nDate Range: {df['date'].min()} to {df['date'].max()}")
    print(f"Total Trading Days: {len(df)}\n")
    
    # Display the data
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.float_format', '{:.2f}'.format)
    print(df.to_string(index=False))
    
    # Calculate some basic statistics
    print(f"\n{'='*70}")
    print(f"Summary Statistics:")
    print(f"{'='*70}")
    print(f"Average Close Price: ${df['close'].mean():.2f}")
    print(f"Highest Price: ${df['high'].max():.2f} on {df.loc[df['high'].idxmax(), 'date']}")
    print(f"Lowest Price: ${df['low'].min():.2f} on {df.loc[df['low'].idxmin(), 'date']}")
    print(f"Total Volume: {df['volume'].sum():,}")
    print(f"Average Daily Volume: {df['volume'].mean():,.0f}")
    
    # Price change
    if len(df) > 1:
        price_change = df['close'].iloc[-1] - df['close'].iloc[0]
        pct_change = (price_change / df['close'].iloc[0]) * 100
        print(f"\nPrice Change: ${price_change:+.2f} ({pct_change:+.2f}%)")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    # Example: Get Apple's recent 10 days of price data
    ticker = "AAPL"
    days = 10
    
    print(f"Fetching last {days} trading days for {ticker}...")
    df = get_recent_prices(ticker, days)
    
    if not df.empty:
        print_price_summary(df, ticker)
    else:
        print(f"\nNo data available for {ticker}. Make sure the database is populated.")
        print("Run: python scripts/update_daily_prices.py")

