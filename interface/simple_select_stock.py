"""
Simple example to query stock price data from DuckDB.
"""
import duckdb
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
import hashlib


def _generate_cache_filename(func_name: str, **kwargs) -> str:
    """
    Generate a cache filename based on function name and parameters.
    
    Args:
        func_name: Name of the function
        **kwargs: Function parameters
    
    Returns:
        Filename in format: function_param1_value1_param2_value2.csv
    """
    # Sort parameters for consistent naming
    sorted_params = sorted(kwargs.items())
    
    # Create filename parts
    parts = [func_name]
    for key, value in sorted_params:
        # Skip db_path and export_to_csv from filename
        if key in ['db_path', 'export_to_csv', 'use_cache']:
            continue
        # Clean value for filename (replace invalid chars)
        clean_value = str(value).replace('/', '-').replace(':', '-').replace(' ', '_')
        parts.append(f"{key}_{clean_value}")
    
    filename = "_".join(parts) + ".csv"
    return filename


def _get_cache_path(filename: str) -> Path:
    """Get the full path for a cache file."""
    cache_dir = Path(__file__).parent.parent / "outputs"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / filename


def _save_to_csv(df: pd.DataFrame, filename: str) -> None:
    """Save DataFrame to CSV in outputs directory."""
    filepath = _get_cache_path(filename)
    df.to_csv(filepath, index=False)
    print(f"✓ Results saved to: {filepath}")


def _load_from_cache(filename: str, max_age_hours: int = 24) -> pd.DataFrame:
    """
    Load DataFrame from cache if it exists and is fresh.
    
    Args:
        filename: Cache filename
        max_age_hours: Maximum age of cache in hours
    
    Returns:
        DataFrame if cache exists and is fresh, None otherwise
    """
    filepath = _get_cache_path(filename)
    
    if not filepath.exists():
        return None
    
    # Check cache age
    file_mtime = datetime.fromtimestamp(filepath.stat().st_mtime)
    age_hours = (datetime.now() - file_mtime).total_seconds() / 3600
    
    if age_hours > max_age_hours:
        print(f"⚠ Cache expired (age: {age_hours:.1f}h > {max_age_hours}h)")
        return None
    
    print(f"✓ Loading from cache (age: {age_hours:.1f}h)")
    return pd.read_csv(filepath)


def get_recent_prices(
    ticker: str, 
    days: int = 10, 
    db_path: str = None,
    export_to_csv: bool = False,
    use_cache: bool = True
):
    """
    Get recent N days of price data for a given ticker.
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        days: Number of recent trading days to retrieve
        db_path: Path to DuckDB database (default: ../data/price/price.duckdb)
        export_to_csv: If True, save results to outputs/ directory
        use_cache: If True, load from cache if available (default: True)
    
    Returns:
        pandas.DataFrame with price data
        
    Example:
        # Get data and save to CSV
        df = get_recent_prices("AAPL", days=20, export_to_csv=True)
        
        # Use cached data if available
        df = get_recent_prices("AAPL", days=20, use_cache=True)
    """
    # Generate cache filename
    cache_filename = _generate_cache_filename(
        "get_recent_prices",
        ticker=ticker,
        days=days
    )
    
    # Try to load from cache if enabled
    if use_cache:
        cached_df = _load_from_cache(cache_filename, max_age_hours=24)
        if cached_df is not None:
            return cached_df
    
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
        
        # Export to CSV if requested
        if export_to_csv and not result_df.empty:
            _save_to_csv(result_df, cache_filename)
        
        return result_df
        
    finally:
        con.close()

def price_change_filter(
    date_from: str, 
    date_to: str,
    price_left_bound: float,
    price_right_bound: float,
    db_path: str = None,
    export_to_csv: bool = False,
    use_cache: bool = True
) -> pd.DataFrame:
    """
    Find all stocks with price change within specified percentage range.
    
    Args:
        date_from: Start date in 'YYYY-MM-DD' format (e.g., '2025-10-10')
        date_to: End date in 'YYYY-MM-DD' format (e.g., '2025-10-15')
        price_left_bound: Minimum percentage change (e.g., -0.05 for -5%)
        price_right_bound: Maximum percentage change (e.g., 0.30 for 30%)
        db_path: Path to DuckDB database (default: ../data/price/price.duckdb)
        export_to_csv: If True, save results to outputs/ directory
        use_cache: If True, load from cache if available (default: True)
    
    Returns:
        pandas.DataFrame with columns: ticker, start_date, start_price, end_date, 
                                       end_price, price_change, pct_change
    
    Example:
        # Find stocks with -5% to 30% change and save to CSV
        df = price_change_filter("2025-10-10", "2025-10-15", -0.05, 0.30, export_to_csv=True)
        
        # Use cached results if available
        df = price_change_filter("2025-11-01", "2025-11-15", 0.10, 0.50, use_cache=True)
        
        # Force fresh query (bypass cache)
        df = price_change_filter("2025-11-01", "2025-11-15", -0.20, -0.05, use_cache=False)
    """
    # Generate cache filename
    cache_filename = _generate_cache_filename(
        "price_change_filter",
        date_from=date_from,
        date_to=date_to,
        price_left_bound=price_left_bound,
        price_right_bound=price_right_bound
    )
    
    # Try to load from cache if enabled
    if use_cache:
        cached_df = _load_from_cache(cache_filename, max_age_hours=24)
        if cached_df is not None:
            return cached_df
    
    if db_path is None:
        # Default path relative to this file
        db_path = Path(__file__).parent.parent / "data" / "price" / "price.duckdb"
    
    # Connect to DuckDB (read-only mode)
    con = duckdb.connect(database=str(db_path), read_only=True)
    
    try:
        query = """
        WITH start_prices AS (
            SELECT 
                ticker,
                dt as start_date,
                close as start_price
            FROM fact_price_daily
            WHERE dt = ?
        ),
        end_prices AS (
            SELECT 
                ticker,
                dt as end_date,
                close as end_price
            FROM fact_price_daily
            WHERE dt = ?
        )
        SELECT 
            s.ticker,
            s.start_date,
            s.start_price,
            e.end_date,
            e.end_price,
            (e.end_price - s.start_price) as price_change,
            ((e.end_price - s.start_price) / s.start_price) as pct_change
        FROM start_prices s
        INNER JOIN end_prices e ON s.ticker = e.ticker
        WHERE ((e.end_price - s.start_price) / s.start_price) >= ?
          AND ((e.end_price - s.start_price) / s.start_price) <= ?
        ORDER BY pct_change DESC
        """
        
        result_df = con.execute(
            query, 
            [date_from, date_to, price_left_bound, price_right_bound]
        ).fetchdf()
        
        if result_df.empty:
            print(f"No stocks found with price change between {price_left_bound*100:.1f}% and {price_right_bound*100:.1f}%")
            print(f"for period {date_from} to {date_to}")
        else:
            print(f"Found {len(result_df)} stocks with price change in range "
                  f"[{price_left_bound*100:.1f}%, {price_right_bound*100:.1f}%]")
        
        # Export to CSV if requested
        if export_to_csv and not result_df.empty:
            _save_to_csv(result_df, cache_filename)
        
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

