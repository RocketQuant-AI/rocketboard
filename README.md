# RocketQuant-AI

A quantitative trading analysis platform for historical stock price data management and analysis.

## Overview

RocketQuant-AI provides a robust infrastructure for:
- **Historical Price Data**: Fetch and store daily stock prices from 2000 onwards
- **Fast Querying**: DuckDB-powered analytical database for quick data retrieval
- **Efficient Storage**: Parquet format for compressed, columnar data storage
- **Simple Interface**: Easy-to-use Python API for querying stock data

## Features

- 📊 **7,194+ tickers** covering S&P 500, NYSE, NASDAQ, and popular ETFs
- 💾 **21.9M+ records** of historical daily price data (2000-2025)
- ⚡ **Sub-second queries** using DuckDB OLAP database
- 🔄 **Async data fetching** for parallel API requests
- 📈 **Price analysis tools** with built-in statistics and visualizations

## Project Structure

```
RocketQuant-AI/
├── data/
│   ├── price/
│   │   ├── daily_stock_price/     # Parquet files (one per ticker)
│   │   └── price.duckdb           # DuckDB analytical database
│   └── stock_assets/              # Ticker lists (S&P 500, NASDAQ, NYSE)
│
├── price/
│   ├── async_history_price.py     # Fetch historical prices
│   ├── init_duckdb.py             # Initialize DuckDB schema
│   └── load_to_duckdb.py          # Load parquet into DuckDB
│
├── interface/
│   ├── simple_select_stock.py     # Query interface (main API)
│   └── test_select.py             # Example usage
│
├── scripts/
│   ├── update_daily_prices.py     # Orchestrate data updates
│   └── update_daily_prices.sh     # Shell wrapper
│
├── utils/
│   └── lookup/
│       └── credentials.py         # API key management
│
└── tests/                         # Test suite
```

## Setup

### Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/RocketQuant-AI.git
cd RocketQuant-AI
```

2. **Create virtual environment**
```bash
# Using uv (recommended)
uv venv --python 3.10
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Or using standard Python
python3.10 -m venv .venv
source .venv/bin/activate
```

3. **Install dependencies**
```bash
# Using uv
uv pip install -e .

# Or using pip
pip install -e .
```

4. **Configure API credentials**

Create a file `utils/lookup/credentials.txt` with your Tiingo API key:
```
your_tiingo_api_key_here
```

Or set as environment variable:
```bash
export SEC_API_KEY=your_tiingo_api_key_here
```

Get a free API key at [Tiingo.com](https://www.tiingo.com/)

## Usage

### Quick Start: Query Stock Data

```python
from interface.simple_select_stock import get_recent_prices

# Get last 10 days of Apple stock prices
df = get_recent_prices("AAPL", days=10)
print(df)

# Get last 30 days of Microsoft
df = get_recent_prices("MSFT", days=30)

# Get last 5 days of Tesla
df = get_recent_prices("TSLA", days=5)
```

### Run Example Script

```bash
cd interface
python simple_select_stock.py
```

Output:
```
======================================================================
Price Data for AAPL
======================================================================

Date Range: 2025-11-04 00:00:00 to 2025-11-17 00:00:00
Total Trading Days: 10

ticker       date   open   high    low  close  adj_close   volume
  AAPL 2025-11-04 268.32 271.49 267.62 270.04     269.78 49274846
  ...

======================================================================
Summary Statistics:
======================================================================
Average Close Price: $270.94
Highest Price: $276.70 on 2025-11-13
Lowest Price: $265.73 on 2025-11-17
Total Volume: 469,263,641
Average Daily Volume: 46,926,364
Price Change: $-2.58 (-0.96%)
======================================================================
```

### Update Historical Data

Fetch latest prices for all tickers:

```bash
# Run the full update pipeline
python scripts/update_daily_prices.py

# Or fetch only (skip DuckDB load)
python scripts/update_daily_prices.py --fetch-only

# Or load only (skip fetch)
python scripts/update_daily_prices.py --load-only
```

### Query DuckDB Directly

```python
import duckdb
from pathlib import Path

db_path = "data/price/price.duckdb"
con = duckdb.connect(db_path, read_only=True)

# Query specific ticker
df = con.execute("""
    SELECT * FROM fact_price_daily
    WHERE ticker = 'AAPL'
    ORDER BY dt DESC
    LIMIT 100
""").fetchdf()

# Calculate moving averages
df = con.execute("""
    SELECT 
        ticker,
        dt,
        close,
        AVG(close) OVER (
            PARTITION BY ticker 
            ORDER BY dt 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) as ma_20
    FROM fact_price_daily
    WHERE ticker = 'AAPL'
    ORDER BY dt DESC
    LIMIT 100
""").fetchdf()

con.close()
```

## Data Management

### Database Schema

**Table: `fact_price_daily`**
```sql
CREATE TABLE fact_price_daily (
    ticker      VARCHAR,      -- Stock symbol (e.g., 'AAPL')
    dt          DATE,         -- Trading date
    open        DOUBLE,       -- Opening price
    high        DOUBLE,       -- Highest price
    low         DOUBLE,       -- Lowest price
    close       DOUBLE,       -- Closing price
    adj_close   DOUBLE,       -- Adjusted close price
    volume      BIGINT,       -- Trading volume
    PRIMARY KEY (ticker, dt)
);
```

### Data Sources

- **API**: [Tiingo](https://www.tiingo.com/) - Financial data API
- **Tickers**: S&P 500, NYSE, NASDAQ listings
- **Coverage**: Daily prices from 2000-01-01 to present
- **Update Frequency**: On-demand (run update script as needed)

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run specific test
pytest tests/test_get_api_key.py

# Or use the convenience script
./run_tests.sh
```

### Code Style

This project follows Python coding conventions:
- Type hints for function signatures
- Docstrings for all public functions
- PEP 8 style guide

## Architecture

### Data Pipeline

```
1. Fetch (async_history_price.py)
   ↓
   Tiingo API → Async requests → Parquet files
   
2. Load (load_to_duckdb.py)
   ↓
   Parquet files → DuckDB database
   
3. Query (simple_select_stock.py)
   ↓
   DuckDB → Pandas DataFrame → Analysis
```

### Design Principles

- **Separation of Concerns**: Raw data (Parquet) separate from analytical DB (DuckDB)
- **Async I/O**: Non-blocking API requests for fast data fetching
- **Columnar Storage**: Parquet format for efficient compression and querying
- **OLAP Database**: DuckDB for fast analytical queries
- **Thread Pool**: Offload blocking I/O to prevent event loop blocking

## Performance

- **Query Speed**: <100ms for typical queries (10-100 days, single ticker)
- **Fetch Speed**: ~7,600 tickers in ~3 minutes (with 10 concurrent connections)
- **Storage**: ~2GB for 7,194 tickers × 6,500 avg records (Parquet compressed)
- **Database**: ~268MB DuckDB file for 21.9M records

## API Reference

### `get_recent_prices(ticker, days, db_path)`

Get recent N days of price data for a ticker.

**Parameters:**
- `ticker` (str): Stock symbol (e.g., 'AAPL', 'MSFT')
- `days` (int): Number of recent trading days to retrieve (default: 10)
- `db_path` (str, optional): Path to DuckDB database

**Returns:**
- `pandas.DataFrame`: Price data with columns: ticker, date, open, high, low, close, adj_close, volume

**Example:**
```python
df = get_recent_prices("AAPL", days=20)
print(df.head())
```

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- [Tiingo](https://www.tiingo.com/) for providing financial data API
- [DuckDB](https://duckdb.org/) for the blazing-fast analytical database
- [Apache Parquet](https://parquet.apache.org/) for efficient columnar storage
- [aiohttp](https://docs.aiohttp.org/) for async HTTP capabilities

## Contact

For questions or feedback, please open an issue on GitHub.

---

**Note**: This is a development/research tool. Not intended for live trading or investment advice. Always do your own research before making investment decisions.

