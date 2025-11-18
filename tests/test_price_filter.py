"""Test the price_change_filter function with caching and export features."""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'interface'))

from simple_select_stock import price_change_filter, get_recent_prices

# Example 1: Find stocks that changed between -5% and 30% (with export and cache)
print("=" * 80)
print("Example 1: Stocks with -5% to 30% change (Nov 4-17, 2025)")
print("=" * 80)
df = price_change_filter("2025-11-04", "2025-11-17", -0.05, 0.30, export_to_csv=True, use_cache=True)
print(f"\n{df.head(10)}")

# Example 2: Find stocks that went up significantly (10% to 50%)
print("\n" + "=" * 80)
print("Example 2: Stocks with strong gains 10% to 50% (Nov 4-17, 2025)")
print("=" * 80)
df_gainers = price_change_filter("2025-11-04", "2025-11-17", 0.10, 0.50, export_to_csv=True)
print(f"\n{df_gainers.head(10)}")

# Example 3: Find stocks that dropped (between -20% and -5%)
print("\n" + "=" * 80)
print("Example 3: Stocks with losses -20% to -5% (Nov 4-17, 2025)")
print("=" * 80)
df_losers = price_change_filter("2025-11-04", "2025-11-17", -0.20, -0.05, export_to_csv=True)
print(f"\n{df_losers.head(10)}")

# Example 4: Test get_recent_prices with export and cache
print("\n" + "=" * 80)
print("Example 4: Get recent 20 days for AAPL (with export and cache)")
print("=" * 80)
df_aapl = get_recent_prices("AAPL", days=20, export_to_csv=True, use_cache=True)
if not df_aapl.empty:
    print(f"Retrieved {len(df_aapl)} days of data for AAPL")
    print(df_aapl.head())

# Example 5: Demonstrate cache loading (should be instant - same query as Example 1)
print("\n" + "=" * 80)
print("Example 5: Load from cache (should be instant)")
print("=" * 80)
df_cached = price_change_filter("2025-11-04", "2025-11-17", -0.05, 0.30, use_cache=True)
print(f"Loaded {len(df_cached)} records from cache")

# Show statistics
if not df.empty:
    print("\n" + "=" * 80)
    print("Statistics for Example 1 (-5% to 30% range):")
    print("=" * 80)
    print(f"Total stocks found: {len(df)}")
    print(f"Average price change: {df['pct_change'].mean()*100:.2f}%")
    print(f"Median price change: {df['pct_change'].median()*100:.2f}%")
    print(f"Best performer: {df.iloc[0]['ticker']} (+{df.iloc[0]['pct_change']*100:.2f}%)")
    print(f"Worst performer: {df.iloc[-1]['ticker']} ({df.iloc[-1]['pct_change']*100:.2f}%)")

print("\n" + "=" * 80)
print("✓ All examples completed! Check outputs/ directory for CSV files.")
print("=" * 80)

