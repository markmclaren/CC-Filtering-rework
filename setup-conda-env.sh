#!/bin/bash

# Setup script for CC-Filtering-rework using conda instead of micromamba
# This approach works better on HPC systems where micromamba might have compatibility issues

echo "Setting up conda environment for CC-Filtering-rework..."

# Download miniconda if not present
if [ ! -d "./miniconda3" ]; then
    echo "Downloading miniconda..."
    curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh -b -p ./miniconda3
    
    # Accept terms of service
    ./miniconda3/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main
    ./miniconda3/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r
fi

# Create environment with relative path
if [ ! -d "./.conda_env" ]; then
    echo "Creating conda environment at ./.conda_env..."
    ./miniconda3/bin/conda create -p ./.conda_env python=3.9 -y
fi

# Install required packages
echo "Installing required packages..."
./.conda_env/bin/pip install simple-slurm pandas pyarrow warcio requests

echo "Environment setup complete!"
echo "To use: ./.conda_env/bin/python your_script.py"