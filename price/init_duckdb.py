"""Initialize DuckDB database for price data."""
import duckdb
from pathlib import Path


def create_price_schema(db_path: str = "../data/price/price.duckdb"):
    """
    Create DuckDB schema for daily price data.
    
    Args:
        db_path: Path to the DuckDB database file
        
    Returns:
        DuckDB connection object
    """
    # Ensure directory exists
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect to database
    con = duckdb.connect(str(db_file))
    
    # Create table matching design.md structure
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_price_daily (
            ticker VARCHAR,
            dt DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            adj_close DOUBLE,
            volume BIGINT,
            PRIMARY KEY (ticker, dt)
        )
    """)
    
    print(f"✓ Database initialized at {db_path}")
    print(f"✓ Table 'fact_price_daily' created")
    
    return con


if __name__ == "__main__":
    # Initialize the database when run directly
    con = create_price_schema()
    
    # Show table info
    result = con.execute("DESCRIBE fact_price_daily").fetchall()
    print("\nTable schema:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")
    
    con.close()

