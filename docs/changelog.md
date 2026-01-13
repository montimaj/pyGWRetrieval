# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-01-12

### Added
- Initial release of pyGWRetrieval
- Core `GroundwaterRetrieval` class for USGS NWIS data retrieval
- Support for multiple data sources: `gwlevels`, `dv`, `iv`
- Spatial query methods: zip code, GeoJSON, shapefile, state, site numbers
- `TemporalAggregator` class for monthly, annual, and custom aggregations
- `GroundwaterPlotter` class for visualization
- Parallel processing with Dask for large datasets
- Command-line interface (CLI)
- Comprehensive example: `full_workflow_csv_zipcodes.py`
- Regional analysis case study for 9 U.S. metropolitan areas
- 15 visualization types for groundwater analysis
- Sustainability index and future projections

### Features
- Buffer-based spatial queries
- CSV zip code batch processing
- Parquet and CSV export formats
- Trend analysis with statistical significance
- Change point detection
- Publication-ready figures at 300 DPI
