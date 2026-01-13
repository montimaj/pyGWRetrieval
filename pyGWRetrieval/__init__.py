"""
pyGWRetrieval - A Python package for retrieving USGS groundwater level data.

This package provides tools to download daily groundwater level data from the
USGS National Water Information System (NWIS) based on geographic inputs such as
zip codes, GeoJSON files, or shapefiles. It supports spatial buffering for point
data and polygon-based queries for area data.

Features:
    - Download groundwater level data by zip code, GeoJSON, or shapefile
    - Apply spatial buffers to point geometries
    - Export data to CSV or Parquet formats
    - Temporal aggregation (monthly, annual, growing season, custom)
    - Time series visualization for wells
    - Parallel processing support via Dask for large datasets

Example:
    >>> from pyGWRetrieval import GroundwaterRetrieval
    >>> gw = GroundwaterRetrieval()
    >>> data = gw.get_data_by_zipcode('89701', buffer_miles=10)
    >>> data.to_csv('groundwater_data.csv')

Author: Sayantan Majumdar
License: MIT
"""

__version__ = "1.0.0"
__author__ = "Sayantan Majumdar"

from .retrieval import GroundwaterRetrieval
from .spatial import (
    get_zipcode_geometry,
    get_geometry_from_geojson,
    get_geometry_from_shapefile,
    buffer_geometry,
    get_bounding_box,
)
from .temporal import TemporalAggregator
from .visualization import GroundwaterPlotter, plot_wells_map, create_comparison_map
from .utils import (
    save_to_csv,
    save_to_parquet,
    validate_date_range,
    setup_logging,
)
from .parallel import (
    check_dask_available,
    get_dask_client,
    parallel_map,
    get_parallel_config,
    to_dask_dataframe,
    from_dask_dataframe,
)

__all__ = [
    "GroundwaterRetrieval",
    "TemporalAggregator",
    "GroundwaterPlotter",
    "plot_wells_map",
    "create_comparison_map",
    "get_zipcode_geometry",
    "get_geometry_from_geojson",
    "get_geometry_from_shapefile",
    "buffer_geometry",
    "get_bounding_box",
    "save_to_csv",
    "save_to_parquet",
    "validate_date_range",
    "setup_logging",
    # Parallel processing
    "check_dask_available",
    "get_dask_client",
    "parallel_map",
    "get_parallel_config",
    "to_dask_dataframe",
    "from_dask_dataframe",
]
