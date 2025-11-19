"""Test the volume_change_filter function."""
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'interface'))

from simple_select_stock import volume_change_filter

# Example 1: Find stocks where recent volume (Nov 10-17) is 2x or more than earlier period (Oct 10-17)
print("=" * 80)
print("Example 1: Volume increased by 2x (Oct 10-17 vs Nov 10-17)")
print("=" * 80)
df1 = volume_change_filter(
    date_x="2025-11-10", 
    date_y="2025-11-17",
    date_z="2025-10-10",
    date_w="2025-10-17",
    volume_ratio=2.0,
    export_to_csv=True,
    use_cache=True
)

if not df1.empty:
    print("\nTop 10 stocks with highest volume increase:")
    print(df1.head(10).to_string(index=False))
    print(f"\n✓ Total stocks found: {len(df1)}")

# Example 2: Find stocks with moderate volume increase (1.5x)
print("\n" + "=" * 80)
print("Example 2: Volume increased by 1.5x (Nov 1-10 vs Nov 11-17)")
print("=" * 80)
df2 = volume_change_filter(
    date_x="2025-11-11",
    date_y="2025-11-17",
    date_z="2025-11-01",
    date_w="2025-11-10",
    volume_ratio=1.5,
    export_to_csv=True
)

if not df2.empty:
    print("\nTop 10 stocks:")
    print(df2.head(10).to_string(index=False))

# Example 3: Week-over-week volume comparison
print("\n" + "=" * 80)
print("Example 3: This week vs last week (3x volume increase)")
print("=" * 80)
df3 = volume_change_filter(
    date_x="2025-11-11",
    date_y="2025-11-17",
    date_z="2025-11-04",
    date_w="2025-11-10",
    volume_ratio=3.0,
    export_to_csv=True
)

if not df3.empty:
    print("\nTop 10 stocks with highest weekly volume surge:")
    print(df3.head(10).to_string(index=False))
    
    # Show statistics
    print("\n" + "=" * 80)
    print("Statistics:")
    print("=" * 80)
    print(f"Total stocks: {len(df3)}")
    print(f"Average volume ratio: {df3['actual_ratio'].mean():.2f}x")
    print(f"Median volume ratio: {df3['actual_ratio'].median():.2f}x")
    print(f"Max volume ratio: {df3['actual_ratio'].max():.2f}x ({df3.iloc[0]['ticker']})")

print("\n" + "=" * 80)
print("✓ All examples completed! Check outputs/ directory for CSV files.")
print("=" * 80)

