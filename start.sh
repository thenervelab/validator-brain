#!/bin/bash
# Start script for the IPFS Service Validator

# Check if poetry is installed
if command -v poetry &> /dev/null; then
    echo "Starting with Poetry..."
    poetry run uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
    exit $?
fi

# Check if we're in a virtual environment
if [[ -z "$VIRTUAL_ENV" ]]; then
    echo "No active virtual environment detected."
    
    if [[ -d "venv" ]]; then
        echo "Found 'venv' directory, activating..."
        source venv/bin/activate
    else
        echo "Creating a new virtual environment..."
        python3 -m venv venv
        source venv/bin/activate
        
        echo "Installing dependencies..."
        pip install -e ".[dev]"
    fi
else
    echo "Using virtual environment: $VIRTUAL_ENV"
    
    if [[ ! -f "$VIRTUAL_ENV/bin/uvicorn" ]]; then
        echo "Installing dependencies in the active virtual environment..."
        pip install -e ".[dev]"
    fi
fi

echo "Starting IPFS Service Validator..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload