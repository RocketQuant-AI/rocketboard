#!/bin/bash
# Update daily stock price data
# 
# Usage:
#   ./update_daily_prices.sh              # Full update: fetch + load
#   ./update_daily_prices.sh --fetch-only # Only fetch data
#   ./update_daily_prices.sh --load-only  # Only load to DuckDB

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Activate virtual environment if it exists
if [ -f "$PROJECT_ROOT/.venv/bin/activate" ]; then
    source "$PROJECT_ROOT/.venv/bin/activate"
    echo "✓ Virtual environment activated"
else
    echo "⚠ Warning: Virtual environment not found at $PROJECT_ROOT/.venv"
fi

# Run the Python script
python "$SCRIPT_DIR/update_daily_prices.py" "$@"

