# Installation Guide

This guide covers different ways to install pyGWRetrieval.

## Requirements

- Python 3.8 or higher
- pip package manager

## Quick Installation

The simplest way to install pyGWRetrieval is using pip:

```bash
pip install pyGWRetrieval
```

## Installation from Source

To install the latest development version from source:

```bash
# Clone the repository
git clone https://github.com/montimaj/pyGWRetrieval.git

# Navigate to the directory
cd pyGWRetrieval

# Install in development mode
pip install -e .
```

## Optional Dependencies

pyGWRetrieval has several optional dependency groups that can be installed based on your needs:

### Visualization Extras

For enhanced visualization capabilities including seaborn and basemaps:

```bash
pip install pyGWRetrieval[viz]
```

This includes:
- `seaborn` - Statistical data visualization
- `contextily` - Basemaps for spatial plots

### Development Dependencies

For development and testing:

```bash
pip install pyGWRetrieval[dev]
```

This includes:
- `pytest` - Testing framework
- `pytest-cov` - Coverage reporting
- `black` - Code formatting
- `flake8` - Linting
- `mypy` - Type checking
- `isort` - Import sorting

### Documentation Dependencies

For building documentation:

```bash
pip install pyGWRetrieval[docs]
```

This includes:
- `sphinx` - Documentation generator
- `sphinx-rtd-theme` - Read the Docs theme
- `myst-parser` - Markdown support
- `sphinx-autodoc-typehints` - Type hint documentation

### All Dependencies

To install all optional dependencies:

```bash
pip install pyGWRetrieval[all]
```

## Verifying Installation

After installation, verify it works correctly:

```python
import pyGWRetrieval
print(pyGWRetrieval.__version__)
```

You can also run a quick test:

```python
from pyGWRetrieval import GroundwaterRetrieval

# This should not raise any errors
gw = GroundwaterRetrieval()
print("Installation successful!")
```

## Troubleshooting

### Common Issues

#### GDAL/GEOS Installation Issues

If you encounter issues installing `geopandas` or `shapely`, you may need to install system dependencies:

**macOS:**
```bash
brew install gdal geos proj
```

**Ubuntu/Debian:**
```bash
sudo apt-get install libgdal-dev libgeos-dev libproj-dev
```

**Windows:**
Consider using conda for easier installation:
```bash
conda install -c conda-forge geopandas
```

#### pgeocode Data Download

The `pgeocode` package downloads postal code data on first use. If you're behind a firewall, you may need to configure proxy settings.

### Getting Help

If you encounter issues:

1. Check the [GitHub Issues](https://github.com/montimaj/pyGWRetrieval/issues)
2. Ensure all dependencies are correctly installed
3. Try creating a fresh virtual environment

## Virtual Environment Setup (Recommended)

It's recommended to use a virtual environment:

```bash
# Create virtual environment
python -m venv pygw_env

# Activate (macOS/Linux)
source pygw_env/bin/activate

# Activate (Windows)
pygw_env\Scripts\activate

# Install package
pip install pyGWRetrieval
```

## Conda Installation

If you prefer conda:

```bash
# Create conda environment
conda create -n pygw python=3.10

# Activate
conda activate pygw

# Install dependencies via conda-forge
conda install -c conda-forge geopandas pandas matplotlib numpy

# Install pyGWRetrieval
pip install pyGWRetrieval
```
