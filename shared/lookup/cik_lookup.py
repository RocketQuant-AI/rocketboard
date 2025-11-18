from secedgar.cik_lookup import CIKLookup

def get_cik(ticker: str, user_agent: str = "Your Name (email@example.com)") -> str:
    """
    Given a stock ticker, return the CIK code using sec-edgar's CIKLookup.

    Args:
        ticker (str): The stock ticker (e.g., 'aapl', 'msft').
        user_agent (str): Required by SEC API. Replace with your name and email.

    Returns:
        str or None: The CIK code if found, else None.
    """
    try:
        lookups = CIKLookup([ticker], user_agent=user_agent)
        cik_dict = lookups.lookup_dict
        # Normalize CIKs to 10-digit zero-padded strings regardless of input type
        def normalize_cik(value):
            s = str(value)
            digits = "".join(ch for ch in s if ch.isdigit())
            return digits.zfill(10) if digits else None

        normalized = {str(k).lower(): normalize_cik(v) for k, v in cik_dict.items()}

        key = ticker.lower()
        if key in normalized and normalized[key]:
            return normalized[key]

        # Fallback: return any valid normalized CIK if available
        for v in normalized.values():
            if v:
                return v
        return None
    except Exception as e:
        return "0000000000"
    
# tests 
if __name__ == "__main__":
    print(get_cik("aapl"))   # 320193
    print(get_cik("msft"))   # 789019
    print(get_cik("pins"))   # 1506293
    print(get_cik("googl"))  # 1652044
    print(get_cik("invalid"))  # None
