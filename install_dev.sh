#!/bin/bash

# Exit on error
set -e

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Upgrade pip and install build tools
echo "Upgrading pip and installing build tools..."
pip install --upgrade pip wheel setuptools

# Install PyYAML first using pre-built wheel
echo "Installing PyYAML..."
pip install --only-binary :all: PyYAML==6.0.1

# Install core dependencies
echo "Installing core dependencies..."
pip install -r requirements.txt

echo "Installing UI dependencies..."
pip install "rich>=13.3.3" "textual>=0.40.0"

echo "Installing system dependencies..."
pip install "psutil>=5.9.0" "py4j>=0.10.9"

echo "Installing data processing dependencies..."
pip install pyarrow PyYAML dataclasses-json typing-extensions pathlib2 click python-dateutil jsonschema fastparquet python-snappy brotli lz4 zstandard

echo "Installing visualization dependencies..."
pip install matplotlib seaborn plotly

echo "Installing testing and development dependencies..."
pip install pytest pytest-cov black flake8 mypy

echo "Installing text processing dependencies..."
pip install beautifulsoup4 types-beautifulsoup4 sentencepiece nltk datasketch

# Install the package in development mode
echo "Installing package in development mode..."
pip install -e .

echo "Setting up environment variables..."
export PYTHONPATH=${PYTHONPATH}:${PWD}
export TRANSFORMERS_CACHE=./model_cache
export TORCH_HOME=./model_cache

echo "Creating model cache directory..."
mkdir -p model_cache

echo "Downloading NLTK data..."
python3 -c "import nltk; nltk.download('punkt')"

echo "Installation complete! To activate the virtual environment, run:"
echo "source venv/bin/activate" 