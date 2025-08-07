#!/bin/bash
# Quick start script for RNAcentral GFF fetcher

echo "RNAcentral GFF Fetcher - Quick Start"
echo "====================================="
echo ""

# Check if PGDATABASE is set
if [ -z "$PGDATABASE" ]; then
    echo "ERROR: PGDATABASE environment variable not set!"
    echo ""
    echo "Please set it with your PostgreSQL connection string:"
    echo "  export PGDATABASE='postgresql://user:password@host:port/database'"
    echo ""
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating Python virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing Python dependencies..."
pip install -q -r requirements.txt

# Make scripts executable
chmod +x download_gff.sh
chmod +x fetch_rnacentral_gff.py
chmod +x test_setup.py
chmod +x monitor.py
chmod +x analyze_coverage.py

echo ""
echo "Setup complete!"
echo ""
echo "Available commands:"
echo "  python test_setup.py        - Test database connection and download"
echo "  python fetch_rnacentral_gff.py - Run the main download script"
echo "  python monitor.py           - Monitor download progress"
echo "  python analyze_coverage.py  - Analyze downloaded files"
echo ""
echo "Starting with setup test..."
echo ""

python test_setup.py
