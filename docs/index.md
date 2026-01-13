# pyGWRetrieval Documentation

[![PyPI version](https://img.shields.io/pypi/v/pyGWRetrieval.svg)](https://pypi.org/project/pyGWRetrieval/)
[![GitHub Pages](https://img.shields.io/badge/docs-GitHub%20Pages-blue?logo=github)](https://montimaj.github.io/pyGWRetrieval/)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Welcome to the pyGWRetrieval documentation!

## Overview

pyGWRetrieval is a Python package for retrieving and analyzing groundwater level data from the USGS National Water Information System (NWIS).

### USGS Data Sources

The package supports three USGS data sources:

| Source | Description |
|--------|-------------|
| `gwlevels` | Field groundwater-level measurements (discrete, manual) |
| `dv` | Daily values (daily summaries from continuous sensors) |
| `iv` | Instantaneous values (15-60 min intervals from sensors) |

## Contents

- [Installation](installation.md) - How to install the package
- [Quick Start](quickstart.md) - Get started quickly with examples
- [CLI Reference](cli.md) - Command line interface documentation
- [API Reference](api_reference.md) - Complete API documentation

## Quick Links

### Data Retrieval

```python
from pyGWRetrieval import GroundwaterRetrieval

# Default: field measurements only
gw = GroundwaterRetrieval(start_date='2020-01-01')
data = gw.get_data_by_zipcode('89701', buffer_miles=10)

# All data sources (gwlevels + dv + iv)
gw = GroundwaterRetrieval(
    start_date='2020-01-01',
    data_sources='all'
)
data = gw.get_data_by_zipcode('89701', buffer_miles=10)
```

### Temporal Aggregation

```python
from pyGWRetrieval import TemporalAggregator

aggregator = TemporalAggregator(data)
monthly = aggregator.to_monthly()
annual = aggregator.to_annual()
```

### Visualization

```python
from pyGWRetrieval import GroundwaterPlotter

plotter = GroundwaterPlotter(data)
fig = plotter.plot_time_series()
```

### Command Line Interface

```bash
# Retrieve data (default: gwlevels only)
pygwretrieval retrieve --zipcode 89701 --buffer 10 --output data.csv

# Retrieve from all USGS sources
pygwretrieval retrieve --zipcode 89701 --data-sources all --output data.csv

# Aggregate data
pygwretrieval aggregate --input data.csv --period monthly --output monthly.csv

# Create visualizations
pygwretrieval plot --input data.csv --type timeseries --output plot.png

# Create spatial map
pygwretrieval map --input data.csv --output map.png --basemap
```

## Features

- **Multiple Data Sources**: Retrieve from gwlevels, daily values, or instantaneous values
- **Spatial Query Support**: Query by zip code, GeoJSON, shapefile, state, or site numbers
- **Flexible Temporal Aggregation**: Monthly, annual, growing season, water year, or custom periods
- **Built-in Visualization**: Time series plots, box plots, heatmaps, and spatial maps
- **Parallel Processing**: Dask-powered parallel processing for large datasets
- **Multiple Export Formats**: CSV and Parquet support
- **Trend Analysis**: Linear trend calculation for each well

## Support

- GitHub Issues: [Report bugs or request features](https://github.com/montimaj/pyGWRetrieval/issues)
- Documentation: [Read the Docs](https://pygwretrieval.readthedocs.io)

## Case Study: Regional Groundwater Analysis

The package includes a comprehensive case study analyzing groundwater trends across nine major U.S. metropolitan areas (New York, Miami, Washington DC, Houston, Boston, Philadelphia, San Francisco, Chicago, Dallas) using 55 years of USGS data.

**Key Results:**
- 7.9M groundwater measurements from 33,018 wells
- 15 publication-ready visualizations
- Sustainability indices and future projections
- Auto-generated analysis report

**Highlights:**
- Dallas shows +10.6 ft/year recovery (best performing)
- Washington DC shows +1.1 ft/year decline (needs attention)
- Miami has most stable conditions

Run `examples/full_workflow_csv_zipcodes.py` to reproduce the analysis. See `examples/output/ANALYSIS_REPORT.md` for the complete report.

## Citation

If you use this package in your research, please cite:

```bibtex
@software{pyGWRetrieval,
  author = {Sayantan Majumdar},
  title = {pyGWRetrieval: Scalable Retrieval and Analysis of USGS Groundwater Level Data},
  year = {2026},
  url = {https://github.com/montimaj/pyGWRetrieval}
}
```

## License

MIT License - see LICENSE file for details.
