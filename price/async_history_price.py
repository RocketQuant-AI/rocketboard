import os
import sys
from pathlib import Path
import pandas as pd
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

from utils.lookup.credentials import API_KEY

# Example ticker lists (you can modify these as needed)
latest_sp500 = pd.read_csv('../data/stock_assets/latest_sp500.csv')['Symbol'].unique().tolist() if os.path.exists('./stock_assets/latest_sp500.csv') else []
nasdaq_tickers = pd.read_csv('../data/stock_assets/nasdaqlisted.csv')['Symbol'].unique().tolist() if os.path.exists('./stock_assets/nasdaqlisted.csv') else []

nyse_tickers = []
if os.path.exists('../data/stock_assets/nyse_tickers.txt'):
    with open('../data/stock_assets/nyse_tickers.txt', 'r') as f:
        for line in f:
            nyse_tickers.append(line.strip())

# Use a smaller test set for now - you can change this to use full lists
# all_tickers = latest_sp500 + nyse_tickers + nasdaq_tickers
# all_tickers = ['AAPL', 'TSLA', 'MSFT', 'GOOGL', 'META']  # Test with a few tickers

# Add specific ETFs requested by user
additional_etfs = ['SPY', 'QQQ', 'SOXL', 'SOXS', 'VOO', 'SOXX', 'XLK']

all_tickers = list(set(latest_sp500 + nyse_tickers + nasdaq_tickers + additional_etfs)) 
all_tickers = [ticker for ticker in all_tickers if not pd.isna(ticker)]
# Convert special characters to - in ticker symbols (e.g., MS^Q becomes MS-Q, BRK.B becomes BRK-B)
all_tickers = [ticker.replace('^', '-').replace('.', '-') for ticker in all_tickers]
print(f"Total tickers to process: {len(all_tickers)}")
os.makedirs('../data/price/daily_stock_price/', exist_ok=True)

# Check which tickers we already have data for
already_tickers = os.listdir('../data/price/daily_stock_price/')
already_tickers = [ticker.replace('.parquet', '') for ticker in already_tickers]

print(all_tickers)
# Filter out tickers we already have
all_tickers = [ticker for ticker in all_tickers if str(ticker).lower() not in [str(t).lower() for t in already_tickers]]
print(f"Tickers to fetch (excluding already downloaded): {len(all_tickers)}")

TIINGO_DAILY_URL = "https://api.tiingo.com/tiingo/daily/{ticker}/prices"

async def fetch_history_price(
    session: aiohttp.ClientSession, 
    ticker: str, 
    api_token: str,
    start_date: str,
    end_date: Optional[str] = None
) -> Tuple[str, Optional[List[Dict]]]:
    """
    Async version of get_history_price function.
    
    Args:
        session: aiohttp ClientSession
        ticker: Stock symbol
        api_token: Tiingo API token
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format (optional)
    
    Returns:
        Tuple of (ticker, data) where data is None if failed
    """
    headers = {
        'Content-Type': 'application/json'
    }
    
    params = {
        'startDate': start_date,
        'token': api_token
    }
    
    if end_date:
        params['endDate'] = end_date
    
    url = TIINGO_DAILY_URL.format(ticker=ticker)
    
    try:
        async with session.get(url, headers=headers, params=params, timeout=60) as resp:
            if resp.status != 200:
                text = await resp.text()
                print(f"Error fetching {ticker}: {resp.status} {text}")
                return ticker, None
            
            data = await resp.json()
            return ticker, data
            
    except Exception as e:
        print(f"Exception for {ticker}: {e}")
        return ticker, None

def save_to_parquet(data: List[Dict], ticker: str, folder: str = "../data/price/daily_stock_price") -> None:
    """
    Save the historical price data to a Parquet file (runs in thread pool).
    
    Args:
        data: List of dictionaries containing historical price data
        ticker: Stock symbol for the filename
        folder: Output folder name
    """
    if not data:
        print(f"No data to save for {ticker}")
        return
        
    try:
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Create directory if it doesn't exist
        os.makedirs(folder, exist_ok=True)
        
        # Save to Parquet
        output_file = f"{folder}/{ticker.lower()}.parquet"
        df.to_parquet(output_file, index=False, engine='pyarrow')
        print(f"Data saved to {output_file} ({len(df)} records)")
        
    except Exception as e:
        print(f"Error saving data for {ticker}: {e}")

async def main():
    """
    Main async function to fetch historical price data for all tickers.
    """
    # Configuration
    start_date = "2000-01-01"  # You can modify this date
    end_date = None  # Leave as None for current date, or set specific date like "2025-01-01"
    max_connections = 10  # Tune this for your network/API limits
    
    print(f"Fetching historical data from {start_date} to {end_date or 'current date'}")
    
    # Create thread pool for CPU-bound CSV writing tasks
    executor = ThreadPoolExecutor(max_workers=5)
    
    async with aiohttp.ClientSession() as session:
        # Use semaphore to limit concurrent connections
        sem = asyncio.Semaphore(max_connections)
        
        async def sem_fetch(ticker: str):
            async with sem:
                return await fetch_history_price(session, ticker, API_KEY, start_date, end_date)
        
        # Create tasks for all tickers
        tasks = [sem_fetch(ticker) for ticker in all_tickers]
        
        # Process results as they complete
        completed = 0
        total = len(all_tickers)
        
        for future in asyncio.as_completed(tasks):
            ticker, data = await future
            completed += 1
            
            if data:
                # Run Parquet saving in thread pool to avoid blocking the event loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(executor, save_to_parquet, data, ticker)
                print(f"✓ Progress: {completed}/{total} - {ticker} completed")
            else:
                print(f"✗ Progress: {completed}/{total} - {ticker} failed")
    
    print(f"\nCompleted fetching historical data for {len(all_tickers)} tickers")

if __name__ == "__main__":
    asyncio.run(main()) 