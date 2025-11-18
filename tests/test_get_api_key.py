import pytest
from utils.lookup.credentials import get_api_key


def test_get_api_key():
    """Test that get_api_key returns a valid API key."""
    api_key = get_api_key()
    assert api_key is not None
    assert isinstance(api_key, str)
    assert len(api_key) > 0


def test_api_key_from_environment(monkeypatch):
    """Test that environment variable takes priority."""
    test_key = "test_env_api_key_12345"
    monkeypatch.setenv("SEC_API_KEY", test_key)
    
    api_key = get_api_key()
    assert api_key == test_key
