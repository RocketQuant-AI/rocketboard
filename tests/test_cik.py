"""Test script for CIK lookup."""
from utils.lookup.cik_lookup import get_cik

def test_cik_lookup():
    """Test that CIK lookup works for a known ticker."""
    cik = get_cik("aapl")
    print(f"AAPL CIK: {cik}")
    assert cik is not None
    assert isinstance(cik, str)
    assert len(cik) > 0

if __name__ == "__main__":
    test_cik_lookup()
    print("✓ CIK lookup test passed!")

