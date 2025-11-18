# Scripts Directory

This directory contains utility scripts for managing the RocketQuant-AI data pipeline.

## update_daily_prices

Updates daily stock price data by fetching from Tiingo API and loading into DuckDB.

### Python Script

```bash
# Full update (fetch + load)
python update_daily_prices.py

# Only fetch new data (save to parquet)
python update_daily_prices.py --fetch-only

# Only load existing parquet files to DuckDB
python update_daily_prices.py --load-only
```

### Shell Script (with auto venv activation)

```bash
# Full update (fetch + load)
./update_daily_prices.sh

# Only fetch new data
./update_daily_prices.sh --fetch-only

# Only load existing parquet files
./update_daily_prices.sh --load-only
```

### What It Does

1. **Fetch Phase** (--fetch-only or default):
   - Runs `price/async_history_price.py`
   - Fetches historical price data from Tiingo API
   - Saves data as parquet files in `data/price/daily_stock_price/`
   - Skips tickers that already have data

2. **Load Phase** (--load-only or default):
   - Runs `price/load_to_duckdb.py`
   - Loads all parquet files into DuckDB
   - Creates/updates `data/price/price.duckdb`
   - Shows summary statistics

3. **Summary**:
   - Displays total tickers, records, and date range
   - Shows sample of recent data

### Requirements

- Python 3.10+
- Virtual environment activated (`.venv`)
- SEC API key configured (see `utils/lookup/credentials.py`)

### Scheduling

You can schedule this script to run daily using cron:

```bash
# Run every day at 6 PM (after market close)
0 18 * * * cd /path/to/RocketQuant-AI && ./scripts/update_daily_prices.sh >> logs/daily_update.log 2>&1
```

### Example Output

```
============================================================
Daily Stock Price Update
Started: 2025-11-18 18:00:00
============================================================

============================================================
STEP 1: Fetching historical price data
============================================================
Running: python /path/to/price/async_history_price.py
Total tickers to process: 500
Tickers to fetch (excluding already downloaded): 10
✓ Progress: 10/10 - AAPL completed
...

✓ Price data fetch completed successfully

============================================================
STEP 2: Loading parquet files into DuckDB
============================================================
Loading 500 parquet files into DuckDB...
✓ Successfully loaded: 500 tickers

Database summary:
  Tickers: 500
  Total rows: 2,500,000
  Date range: 2000-01-01 to 2025-11-18

✓ Data loaded into DuckDB successfully

============================================================
SUMMARY: Database Statistics
============================================================

  Total Tickers: 500
  Total Records: 2,500,000
  Date Range: 2000-01-01 to 2025-11-18

  Recent Data (sample):
  ticker  latest_date   days
    AAPL   2025-11-18   5000
    MSFT   2025-11-18   5000
    GOOGL  2025-11-18   4000
    ...

============================================================
✓ Update completed successfully!
Finished: 2025-11-18 18:05:30
============================================================
```

