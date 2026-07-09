# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0.post1] - 2026-07-08

### Added
- Archived the release on Zenodo with a citable DOI. Added the Zenodo DOI badge
  and a `doi` field in the BibTeX citation across the README and documentation
  (`index`, `quickstart`, `case_study`, `analysis_report`).

_Post-release: documentation/metadata only; no code or API changes._

## [0.2.0] - 2026-07-08

### Added
- **Water-table depth interpolation** (`WaterTableInterpolator`,
  `InterpolationResult`). Turn point observations into gridded water-table
  depth maps for a chosen window (`period='all'|'monthly'|'annual'|'custom'`)
  at a user-specified resolution **in meters**. Methods: `idw` (pure NumPy,
  always available), `kriging` (ordinary, via `pykrige`), and `linear`/`cubic`/
  `nearest`/`rbf` (via `scipy`). Observations are projected to a metric CRS
  (auto-UTM by default), interpolated onto a grid, and clipped to an optional
  boundary. Results support `.plot()`, `.to_geotiff()`, and `.to_xarray()`.
  New optional dependency group `pyGWRetrieval[interp]`
  (`scipy`, `pykrige`, `rasterio`, `xarray`). The Delaunay methods
  (`linear`/`cubic`) can backfill cells beyond the wells' convex hull via
  `fill_outside='nearest'` (or `'idw'`). Also adds `idw_at_points`, a
  scattered-point IDW estimator with leave-one-out support — used, for example,
  to keep unclassified/mixed wells whose depth-to-water is consistent with the
  water-table surface defined by the confirmed unconfined wells instead of
  discarding them.
- **Aquifer information / unconfined filtering** — `get_aquifer_info()` returns
  each well's `aqfr_type_cd` (`'U'` unconfined, `'C'` confined, `'M'` mixed) plus
  aquifer/well-depth attributes, and merges them into `wells`. Water-table depth
  is only meaningful for unconfined aquifers, so filter with
  `aqfr_type_cd == 'U'` before mapping.
- **`parameter_cd` column** on retrieved data records which USGS parameter each
  value came from — filter on it (e.g. `data['parameter_cd'] == '72019'`) to
  keep wells on the same footing (depth-to-water vs. water-level elevation).
- Automatic bounding-box handling for large spatial queries — queries via
  `get_data_by_shapefile`, `get_data_by_geojson`, and buffered zip codes now
  work for whole hydrologic regions / large basins with no API change (native
  request chunking; a legacy sub-box tiler covers the NWIS fallback).
- Upper Colorado River Basin (WBD HU2 = 14) example in `examples/ucrc/`: a
  long-term mean water-table-depth map at 1 km resolution with a five-method
  interpolation comparison, an unconfined-aquifer gap-fill, and a companion
  `compare_thresholds.py` gap-fill sensitivity comparison. See the
  [examples README](https://github.com/montimaj/pyGWRetrieval/blob/main/examples/README.md).
- Unit tests for interpolation (grid, IDW, `idw_at_points`, `fill_outside`),
  waterdata schema mapping, value coalescing, and bbox tiling.

### Changed
- **Migrated to the modern USGS Water Data OGC API** (`dataretrieval >= 1.2.0`,
  the `waterdata` module). Well discovery, daily values, instantaneous values,
  field (gwlevels) measurements, and site/aquifer info now use
  `get_monitoring_locations` / `get_daily` / `get_continuous` /
  `get_field_measurements`. This **fixes field-measurement retrieval** (the
  deprecated NWIS `gwlevels` endpoint returned an HTML notice instead of data)
  and adds native large-area request chunking and aquifer attributes
  (`aqfr_type_cd`) on discovered wells. The deprecated NWIS endpoints are
  retained only as an automatic fallback.

### Fixed
- Field (`gwlevels`) measurement retrieval, which returned an HTML notice
  instead of data from the deprecated NWIS endpoint (now via
  `waterdata.get_field_measurements`).
- `get_data_by_state` now maps two-letter USPS codes (e.g. `'NV'`) to the full
  state name required by the OGC API; previously it returned no wells.
- CLI `--version` now reports the installed package version (was hardcoded to
  `0.1.0`).

### Breaking
- **`site_no` now carries the full monitoring-location id** (e.g.
  `USGS-393000119000001`) rather than the bare site number.
- **Default `data_sources` is now `'dv'` (daily values)** instead of `'all'`.
  Pass `data_sources='gwlevels'`, `'iv'`, `'all'`, or a list to override
  (Python API and `pygwretrieval retrieve` CLI).
- **Retrieved-data schema changed** — records are standardized to `site_no`,
  `datetime`, `value`/`lev_va`, and `parameter_cd` across all sources; the
  NWIS-only columns (`lev_tm`, `lev_acy_cd`, `lev_src_cd`, `lev_meth_cd`,
  `lev_status_cd`) are no longer present.
- Requires `dataretrieval >= 1.2.0` (was `>= 1.0.0`).
- **Minimum Python is now 3.10** (was 3.8), following `dataretrieval >= 1.2.0`.

_These breaking changes are acceptable under a `0.x` minor release; on a
post-1.0 project they would warrant a major version bump._

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
