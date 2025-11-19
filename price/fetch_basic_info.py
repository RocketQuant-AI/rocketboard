"""
Fetch basic company information from Finnhub API for all tickers.
Rate limit: 60 requests per minute (free tier).
"""
import os
import sys
import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Tuple
import time
from collections import deque

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Finnhub API configuration
FINNHUB_API_URL = "https://finnhub.io/api/v1/stock/profile2"
FINNHUB_API_TOKEN = "d4e3071r01qmhtc6t2l0d4e3071r01qmhtc6t2lg"

# Rate limiting: 60 requests per minute = 1 request per second
RATE_LIMIT = 60  # requests per minute
REQUEST_INTERVAL = 60.0 / RATE_LIMIT  # seconds between requests


class RateLimiter:
    """Rate limiter to ensure we don't exceed API limits."""
    
    def __init__(self, max_requests: int, time_window: float):
        """
        Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed
            time_window: Time window in seconds
        """
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = deque()
    
    async def acquire(self):
        """Wait if necessary to stay within rate limit."""
        now = time.time()
        
        # Calculate minimum time between requests
        min_interval = self.time_window / self.max_requests
        
        # If we have recent requests, ensure minimum interval
        if self.requests:
            time_since_last = now - self.requests[-1]
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                await asyncio.sleep(sleep_time)
                now = time.time()
        
        # Remove old requests outside the time window
        while self.requests and self.requests[0] < now - self.time_window:
            self.requests.popleft()
        
        # If we've hit the limit, wait for the oldest request to expire
        if len(self.requests) >= self.max_requests:
            sleep_time = self.requests[0] + self.time_window - now + 0.1  # Add 100ms buffer
            if sleep_time > 0:
                print(f"⏳ Rate limit reached, waiting {sleep_time:.1f}s...")
                await asyncio.sleep(sleep_time)
                now = time.time()
                # Clean up again after waiting
                while self.requests and self.requests[0] < now - self.time_window:
                    self.requests.popleft()
        
        # Record this request
        self.requests.append(time.time())


async def fetch_stock_profile(
    session: aiohttp.ClientSession,
    ticker: str,
    rate_limiter: RateLimiter
) -> Tuple[str, Optional[Dict]]:
    """
    Fetch stock profile data from Finnhub API.
    
    Args:
        session: aiohttp ClientSession
        ticker: Stock symbol
        rate_limiter: RateLimiter instance
    
    Returns:
        Tuple of (ticker, data) where data is None if failed
    """
    # Wait for rate limiter
    await rate_limiter.acquire()
    
    params = {
        'symbol': ticker,
        'token': FINNHUB_API_TOKEN
    }
    
    try:
        async with session.get(FINNHUB_API_URL, params=params, timeout=30) as resp:
            if resp.status != 200:
                text = await resp.text()
                print(f"✗ Error fetching {ticker}: HTTP {resp.status} - {text}")
                return ticker, None
            
            data = await resp.json()
            
            # Check if we got valid data (Finnhub returns empty dict for invalid tickers)
            if not data or not data.get('name'):
                print(f"✗ No data for {ticker}")
                return ticker, None
            
            return ticker, data
            
    except asyncio.TimeoutError:
        print(f"✗ Timeout for {ticker}")
        return ticker, None
    except Exception as e:
        print(f"✗ Exception for {ticker}: {e}")
        return ticker, None


def save_to_parquet(data: Dict, ticker: str, folder: str = "../data/basic_info") -> None:
    """
    Save the stock profile data to a Parquet file.
    
    Args:
        data: Dictionary containing stock profile data
        ticker: Stock symbol for the filename
        folder: Output folder name
    """
    if not data:
        print(f"✗ No data to save for {ticker}")
        return
    
    try:
        # Convert to DataFrame (single row)
        df = pd.DataFrame([data])
        
        # Create directory if it doesn't exist
        output_dir = Path(__file__).parent / folder
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Save to Parquet
        output_file = output_dir / f"{ticker.lower()}.parquet"
        df.to_parquet(output_file, index=False, engine='pyarrow')
        print(f"✓ Saved {ticker}: {data.get('name', 'N/A')}")
        
    except Exception as e:
        print(f"✗ Error saving data for {ticker}: {e}")


async def main():
    """
    Main async function to fetch stock profile data for all tickers.
    """
    # Load ticker lists
    sp500_path = Path(__file__).parent.parent / 'data' / 'stock_assets' / 'latest_sp500.csv'
    nasdaq_path = Path(__file__).parent.parent / 'data' / 'stock_assets' / 'nasdaqlisted.csv'
    nyse_path = Path(__file__).parent.parent / 'data' / 'stock_assets' / 'nyse_tickers.txt'
    
    all_tickers = []
    
    # Load S&P 500
    if sp500_path.exists():
        sp500_df = pd.read_csv(sp500_path)
        all_tickers.extend(sp500_df['Symbol'].unique().tolist())
    
    # Load NASDAQ
    if nasdaq_path.exists():
        nasdaq_df = pd.read_csv(nasdaq_path)
        all_tickers.extend(nasdaq_df['Symbol'].unique().tolist())
    
    # Load NYSE
    if nyse_path.exists():
        with open(nyse_path, 'r') as f:
            nyse_tickers = [line.strip() for line in f if line.strip()]
            all_tickers.extend(nyse_tickers)
    
    # Add specific ETFs
    additional_etfs = ['SPY', 'QQQ', 'SOXL', 'SOXS', 'VOO', 'SOXX', 'XLK', 'IBIT']
    all_tickers.extend(additional_etfs)
    
    # Remove duplicates and clean
    all_tickers = list(set(all_tickers))
    all_tickers = [ticker for ticker in all_tickers if ticker and not pd.isna(ticker)]
    
    # Convert special characters
    all_tickers = [ticker.replace('^', '-').replace('.', '-') for ticker in all_tickers]
    
    print(f"Total tickers to process: {len(all_tickers)}")
    
    # Check which tickers we already have
    output_dir = Path(__file__).parent / "../data/basic_info"
    if output_dir.exists():
        already_fetched = [f.stem for f in output_dir.glob("*.parquet")]
        all_tickers = [t for t in all_tickers if t.lower() not in [a.lower() for a in already_fetched]]
        print(f"Already have {len(already_fetched)} tickers")
    
    print(f"Tickers to fetch: {len(all_tickers)}")
    
    if not all_tickers:
        print("✓ All tickers already fetched!")
        return
    
    # Create rate limiter (60 requests per minute)
    rate_limiter = RateLimiter(max_requests=RATE_LIMIT, time_window=60.0)
    
    print(f"\nStarting fetch with rate limit: {RATE_LIMIT} requests/minute")
    print(f"Estimated time: {len(all_tickers) / RATE_LIMIT:.1f} minutes\n")
    
    start_time = datetime.now()
    
    async with aiohttp.ClientSession() as session:
        completed = 0
        total = len(all_tickers)
        
        for ticker in all_tickers:
            ticker_data = await fetch_stock_profile(session, ticker, rate_limiter)
            ticker_symbol, data = ticker_data
            
            completed += 1
            
            if data:
                # Save synchronously (fast enough for single writes)
                save_to_parquet(data, ticker_symbol)
            
            # Progress update every 10 tickers
            if completed % 10 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = completed / elapsed if elapsed > 0 else 0
                eta = (total - completed) / rate if rate > 0 else 0
                print(f"\n📊 Progress: {completed}/{total} ({completed/total*100:.1f}%) "
                      f"- Rate: {rate*60:.1f}/min - ETA: {eta/60:.1f}min\n")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*70}")
    print(f"✓ Completed fetching basic info for {len(all_tickers)} tickers")
    print(f"Duration: {duration}")
    print(f"Average rate: {len(all_tickers) / duration.total_seconds() * 60:.1f} requests/min")
    print(f"{'='*70}")


if __name__ == "__main__":
    asyncio.run(main())

