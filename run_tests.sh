#!/bin/bash
# Test runner script that sets PYTHONPATH automatically

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Set PYTHONPATH to the project root
export PYTHONPATH="$SCRIPT_DIR"

# Run pytest with all arguments passed to this script
pytest "$@"

