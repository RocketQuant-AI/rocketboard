"""
Test fetch_basic_info with a small subset of tickers to verify rate limiter.
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

# Rate limiting: 60 requests per minute
RATE_LIMIT = 60  # requests per minute


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
        
        # Calculate minimum time between requests (e.g., 60 req/min = 1 sec between requests)
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
                print(f"✗ Error fetching {ticker}: HTTP {resp.status}")
                return ticker, None
            
            data = await resp.json()
            
            # Check if we got valid data
            if not data or not data.get('name'):
                print(f"✗ No data for {ticker}")
                return ticker, None
            
            return ticker, data
            
    except Exception as e:
        print(f"✗ Exception for {ticker}: {e}")
        return ticker, None


def save_to_parquet(data: Dict, ticker: str, folder: str = "../data/basic_info") -> None:
    """Save the stock profile data to a Parquet file."""
    if not data:
        return
    
    try:
        df = pd.DataFrame([data])
        output_dir = Path(__file__).parent / folder
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{ticker.lower()}.parquet"
        df.to_parquet(output_file, index=False, engine='pyarrow')
        print(f"✓ {ticker}: {data.get('name', 'N/A')} (Market Cap: ${data.get('marketCapitalization', 0):.1f}M)")
    except Exception as e:
        print(f"✗ Error saving {ticker}: {e}")


async def main():
    """Test with a small subset of tickers."""
    # Test with 10 well-known tickers
    test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'V', 'WMT']
    
    print(f"Testing rate limiter with {len(test_tickers)} tickers")
    print(f"Rate limit: {RATE_LIMIT} requests/minute\n")
    
    # Create rate limiter
    rate_limiter = RateLimiter(max_requests=RATE_LIMIT, time_window=60.0)
    
    start_time = datetime.now()
    
    async with aiohttp.ClientSession() as session:
        for i, ticker in enumerate(test_tickers, 1):
            request_start = time.time()
            
            ticker_symbol, data = await fetch_stock_profile(session, ticker, rate_limiter)
            
            if data:
                save_to_parquet(data, ticker_symbol)
            
            request_time = time.time() - request_start
            print(f"   Request {i}/{len(test_tickers)} took {request_time:.2f}s\n")
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print(f"\n{'='*70}")
    print(f"✓ Completed test")
    print(f"Duration: {duration}")
    print(f"Average rate: {len(test_tickers) / duration.total_seconds() * 60:.1f} requests/min")
    print(f"{'='*70}")


if __name__ == "__main__":
    asyncio.run(main())

