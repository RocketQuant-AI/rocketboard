"""Credentials management for API keys."""
import os
from pathlib import Path


def get_api_key() -> str:
    """
    Get API key from environment variable or credentials file.
    Priority: 1) SEC_API_KEY env var, 2) credentials.txt
    """
    # Try environment variable first
    api_key = os.getenv('SEC_API_KEY')
    if api_key:
        return api_key.strip()
    
    # Fall back to credentials file
    credentials_path = Path(__file__).parent / 'credentials.txt'
    if credentials_path.exists():
        return credentials_path.read_text().strip()
    
    raise FileNotFoundError(
        "API key not found. Set SEC_API_KEY environment variable "
        "or create utils/lookup/credentials.txt"
    )


# Module-level constant for backward compatibility
API_KEY = get_api_key()
