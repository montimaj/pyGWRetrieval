# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-12

### Added
- Initial release of pyGWRetrieval
- Core `GroundwaterRetrieval` class for data retrieval from USGS NWIS
  - Query by zip code with buffer
  - Query by GeoJSON file
  - Query by shapefile
  - Query by state code
  - Query by specific site numbers
- Spatial processing module (`spatial.py`)
  - Zip code to coordinates conversion
  - GeoJSON file reading
  - Shapefile reading
  - Buffer geometry operations
  - Bounding box extraction
- Temporal aggregation module (`temporal.py`)
  - Monthly aggregation
  - Annual aggregation
  - Water year aggregation
  - Growing season aggregation
  - Custom period aggregation
  - Weekly aggregation
  - Trend analysis
- Visualization module (`visualization.py`)
  - Time series plots
  - Single well detailed plots
  - Multi-well comparison plots
  - Monthly box plots
  - Annual summary plots
  - Year-month heatmaps
  - Spatial distribution maps
- Utility functions (`utils.py`)
  - CSV export/import
  - Parquet export/import
  - Date validation
  - Data cleaning
  - Coverage statistics
- Comprehensive documentation
  - Installation guide
  - Quick start guide
  - API reference
- Example scripts
  - Basic usage
  - Advanced spatial queries
  - Temporal analysis
- Unit tests for core functionality

### Dependencies
- dataretrieval >= 1.0.0
- pandas >= 1.3.0
- geopandas >= 0.10.0
- shapely >= 1.8.0
- pyproj >= 3.0.0
- pgeocode >= 0.3.0
- matplotlib >= 3.4.0
- numpy >= 1.20.0
