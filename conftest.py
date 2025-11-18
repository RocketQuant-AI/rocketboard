"""
Pytest configuration file to ensure the project root is in the Python path.
This allows tests to import from 'shared', 'price', and 'earnings' modules.
"""
import sys
from pathlib import Path

# Add the project root to sys.path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

