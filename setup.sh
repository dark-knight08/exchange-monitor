#!/bin/bash

# HKEX Market Monitor Setup Script
# Usage: ./setup.sh

set -e

echo "==================================="
echo "HKEX Market Monitor Setup"
echo "==================================="
echo ""

# Check Python version
python_version=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python version: $python_version"

# Create virtual environment
echo ""
echo "Creating virtual environment..."
python3 -m venv venv
echo "✓ Virtual environment created"

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source venv/bin/activate
echo "✓ Virtual environment activated"

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt
echo "✓ Dependencies installed"

# Create directories
echo ""
echo "Creating directories..."
mkdir -p data logs
echo "✓ Directories created"

# Initialize database
echo ""
echo "Initializing database..."
python main.py init
echo "✓ Database initialized"

# Run test collection
echo ""
echo "Running test data collection..."
python main.py collect --asset-classes equity
echo "✓ Test collection complete"

echo ""
echo "==================================="
echo "Setup Complete!"
echo "==================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Start the API server:"
echo "   python main.py api"
echo ""
echo "2. Open the dashboard:"
echo "   cd web && python3 -m http.server 8080"
echo "   Then open http://localhost:8080"
echo ""
echo "3. Or start the full system:"
echo "   python main.py run"
echo ""
echo "API Documentation: http://localhost:8000/docs"
echo ""
