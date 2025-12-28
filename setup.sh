#!/bin/bash

# Setup script for Snowflake SaaS Analytics Pipeline
# This script helps you get started quickly

set -e

echo "=================================================="
echo "  Snowflake SaaS Analytics Pipeline Setup"
echo "=================================================="
echo ""

# Check Python version
echo "Checking Python version..."
if ! command -v python3 > /dev/null 2>&1; then
    echo "❌ Python 3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo "✅ Python $PYTHON_VERSION found"
echo ""

# Create virtual environment
echo "Creating virtual environment..."
if [ -d "venv" ]; then
    echo "⚠️  Virtual environment already exists. Skipping..."
else
    python3 -m venv venv
    echo "✅ Virtual environment created"
fi
echo ""

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate
echo "✅ Virtual environment activated"
echo ""

# Install dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "✅ Dependencies installed"
echo ""

# Generate synthetic data
echo "Generating synthetic data..."
python3 scripts/generate_synthetic_data.py
echo "✅ Synthetic data generated"
echo ""

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file from template..."
    cp config.env.template .env
    echo "✅ .env file created"
    echo ""
    echo "⚠️  IMPORTANT: Edit .env file and add your Snowflake credentials!"
    echo ""
else
    echo "⚠️  .env file already exists. Skipping..."
    echo ""
fi

# Display next steps
echo "=================================================="
echo "  Setup Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Edit .env file and add your Snowflake credentials:"
echo "   nano .env"
echo ""
echo "2. Load environment variables:"
echo "   source .env"
echo ""
echo "3. Setup Snowflake database and load data:"
echo "   python3 scripts/snowflake_setup.py"
echo ""
echo "4. Launch the dashboard:"
echo "   streamlit run dashboard/app.py"
echo ""
echo "For more information, see README.md"
echo "=================================================="
