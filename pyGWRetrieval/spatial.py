"""
Spatial processing module for pyGWRetrieval.

This module provides functions for handling spatial data including:
- Converting zip codes to geographic coordinates
- Reading and processing GeoJSON files
- Reading and processing shapefiles
- Creating buffer zones around point geometries
- Extracting bounding boxes from geometries

Dependencies:
    - geopandas
    - shapely
    - pgeocode
    - pyproj
"""

import logging
from pathlib import Path
from typing import Optional, Tuple, Union, List

import geopandas as gpd
import numpy as np
from shapely.geometry import Point, Polygon, MultiPolygon, box
from shapely.ops import unary_union
import pgeocode
from pyproj import CRS, Transformer

logger = logging.getLogger(__name__)


def get_zipcode_geometry(
    zipcode: str,
    country: str = "US"
) -> Tuple[Point, dict]:
    """
    Get the centroid point geometry for a given zip code.

    Parameters
    ----------
    zipcode : str
        The zip code to look up (e.g., '89701').
    country : str, optional
        The country code, by default "US".

    Returns
    -------
    Tuple[Point, dict]
        A tuple containing:
        - Point geometry of the zip code centroid
        - Dictionary with additional location info (city, state, etc.)

    Raises
    ------
    ValueError
        If the zip code is not found or invalid.

    Examples
    --------
    >>> point, info = get_zipcode_geometry('89701')
    >>> print(f"Coordinates: {point.x}, {point.y}")
    >>> print(f"Location: {info['place_name']}, {info['state_name']}")
    """
    nomi = pgeocode.Nominatim(country)
    result = nomi.query_postal_code(zipcode)
    
    if np.isnan(result.latitude) or np.isnan(result.longitude):
        raise ValueError(f"Invalid or unknown zip code: {zipcode}")
    
    point = Point(result.longitude, result.latitude)
    
    info = {
        'zipcode': zipcode,
        'place_name': result.place_name,
        'state_name': result.state_name,
        'state_code': result.state_code,
        'county_name': result.county_name,
        'latitude': result.latitude,
        'longitude': result.longitude,
    }
    
    logger.info(f"Found zip code {zipcode}: {info['place_name']}, {info['state_name']}")
    
    return point, info


def get_geometry_from_geojson(
    filepath: Union[str, Path],
    layer: Optional[str] = None
) -> gpd.GeoDataFrame:
    """
    Read geometry from a GeoJSON file.

    Parameters
    ----------
    filepath : Union[str, Path]
        Path to the GeoJSON file.
    layer : Optional[str], optional
        Layer name for multi-layer files, by default None.

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the geometries from the file.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the file contains no valid geometries.

    Examples
    --------
    >>> gdf = get_geometry_from_geojson('study_area.geojson')
    >>> print(f"Number of features: {len(gdf)}")
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"GeoJSON file not found: {filepath}")
    
    gdf = gpd.read_file(filepath, layer=layer)
    
    if gdf.empty or gdf.geometry.isna().all():
        raise ValueError(f"No valid geometries found in: {filepath}")
    
    # Ensure CRS is WGS84
    if gdf.crs is None:
        logger.warning("No CRS found, assuming WGS84 (EPSG:4326)")
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        logger.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
        gdf = gdf.to_crs(epsg=4326)
    
    logger.info(f"Loaded {len(gdf)} features from {filepath}")
    
    return gdf


def get_geometry_from_shapefile(
    filepath: Union[str, Path]
) -> gpd.GeoDataFrame:
    """
    Read geometry from a shapefile.

    Parameters
    ----------
    filepath : Union[str, Path]
        Path to the shapefile (.shp file).

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame containing the geometries from the shapefile.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the file contains no valid geometries.

    Examples
    --------
    >>> gdf = get_geometry_from_shapefile('basins.shp')
    >>> print(f"Geometry types: {gdf.geom_type.unique()}")
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"Shapefile not found: {filepath}")
    
    gdf = gpd.read_file(filepath)
    
    if gdf.empty or gdf.geometry.isna().all():
        raise ValueError(f"No valid geometries found in: {filepath}")
    
    # Ensure CRS is WGS84
    if gdf.crs is None:
        logger.warning("No CRS found, assuming WGS84 (EPSG:4326)")
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        logger.info(f"Reprojecting from {gdf.crs} to EPSG:4326")
        gdf = gdf.to_crs(epsg=4326)
    
    logger.info(f"Loaded {len(gdf)} features from {filepath}")
    
    return gdf


def buffer_geometry(
    geometry: Union[Point, Polygon, MultiPolygon, gpd.GeoDataFrame],
    buffer_miles: float,
    cap_style: str = 'round'
) -> Union[Polygon, MultiPolygon, gpd.GeoDataFrame]:
    """
    Create a buffer around a geometry using a specified distance in miles.

    Parameters
    ----------
    geometry : Union[Point, Polygon, MultiPolygon, gpd.GeoDataFrame]
        The input geometry to buffer.
    buffer_miles : float
        Buffer distance in miles.
    cap_style : str, optional
        Buffer cap style ('round', 'flat', 'square'), by default 'round'.

    Returns
    -------
    Union[Polygon, MultiPolygon, gpd.GeoDataFrame]
        Buffered geometry in WGS84 coordinates.

    Examples
    --------
    >>> from shapely.geometry import Point
    >>> point = Point(-119.5, 39.5)
    >>> buffered = buffer_geometry(point, buffer_miles=10)
    >>> print(f"Buffered area bounds: {buffered.bounds}")
    """
    # Convert miles to meters
    buffer_meters = buffer_miles * 1609.34
    
    cap_style_map = {'round': 1, 'flat': 2, 'square': 3}
    cap = cap_style_map.get(cap_style, 1)
    
    if isinstance(geometry, gpd.GeoDataFrame):
        # Get centroid for projection
        centroid = geometry.geometry.unary_union.centroid
        center_lon, center_lat = centroid.x, centroid.y
        
        # Create appropriate UTM projection
        utm_crs = _get_utm_crs(center_lon, center_lat)
        
        # Project, buffer, and reproject
        gdf_utm = geometry.to_crs(utm_crs)
        gdf_utm['geometry'] = gdf_utm.geometry.buffer(buffer_meters, cap_style=cap)
        buffered_gdf = gdf_utm.to_crs(epsg=4326)
        
        logger.info(f"Created {buffer_miles} mile buffer around GeoDataFrame")
        return buffered_gdf
    
    else:
        # Handle single geometry
        if isinstance(geometry, Point):
            center_lon, center_lat = geometry.x, geometry.y
        else:
            centroid = geometry.centroid
            center_lon, center_lat = centroid.x, centroid.y
        
        # Create appropriate UTM projection
        utm_crs = _get_utm_crs(center_lon, center_lat)
        
        # Create transformers
        to_utm = Transformer.from_crs(CRS.from_epsg(4326), utm_crs, always_xy=True)
        to_wgs84 = Transformer.from_crs(utm_crs, CRS.from_epsg(4326), always_xy=True)
        
        # Transform to UTM
        if isinstance(geometry, Point):
            x_utm, y_utm = to_utm.transform(geometry.x, geometry.y)
            geom_utm = Point(x_utm, y_utm)
        else:
            # Transform polygon coordinates
            geom_utm = _transform_geometry(geometry, to_utm)
        
        # Buffer in UTM
        buffered_utm = geom_utm.buffer(buffer_meters, cap_style=cap)
        
        # Transform back to WGS84
        buffered = _transform_geometry(buffered_utm, to_wgs84)
        
        logger.info(f"Created {buffer_miles} mile buffer around geometry")
        return buffered


def _get_utm_crs(longitude: float, latitude: float) -> CRS:
    """
    Get the appropriate UTM CRS for a given longitude/latitude.

    Parameters
    ----------
    longitude : float
        Longitude in decimal degrees.
    latitude : float
        Latitude in decimal degrees.

    Returns
    -------
    CRS
        PyProj CRS object for the appropriate UTM zone.
    """
    utm_zone = int((longitude + 180) / 6) + 1
    hemisphere = 'north' if latitude >= 0 else 'south'
    
    if hemisphere == 'north':
        epsg = 32600 + utm_zone
    else:
        epsg = 32700 + utm_zone
    
    return CRS.from_epsg(epsg)


def _transform_geometry(geometry, transformer):
    """
    Transform a geometry using a pyproj Transformer.

    Parameters
    ----------
    geometry : shapely.geometry
        Input geometry to transform.
    transformer : pyproj.Transformer
        Transformer object for coordinate conversion.

    Returns
    -------
    shapely.geometry
        Transformed geometry.
    """
    from shapely.ops import transform
    
    def transform_coords(x, y):
        return transformer.transform(x, y)
    
    return transform(transform_coords, geometry)


def get_bounding_box(
    geometry: Union[Point, Polygon, MultiPolygon, gpd.GeoDataFrame]
) -> Tuple[float, float, float, float]:
    """
    Get the bounding box of a geometry.

    Parameters
    ----------
    geometry : Union[Point, Polygon, MultiPolygon, gpd.GeoDataFrame]
        Input geometry.

    Returns
    -------
    Tuple[float, float, float, float]
        Bounding box as (min_lon, min_lat, max_lon, max_lat).

    Examples
    --------
    >>> bbox = get_bounding_box(buffered_geometry)
    >>> min_lon, min_lat, max_lon, max_lat = bbox
    """
    if isinstance(geometry, gpd.GeoDataFrame):
        bounds = geometry.total_bounds
    else:
        bounds = geometry.bounds
    
    return tuple(bounds)


def get_geometry_type(
    geometry: Union[Point, Polygon, MultiPolygon, gpd.GeoDataFrame]
) -> str:
    """
    Determine if geometry is point-based or polygon-based.

    Parameters
    ----------
    geometry : Union[Point, Polygon, MultiPolygon, gpd.GeoDataFrame]
        Input geometry to classify.

    Returns
    -------
    str
        'point' for Point geometries, 'polygon' for Polygon/MultiPolygon.
    """
    if isinstance(geometry, gpd.GeoDataFrame):
        geom_types = geometry.geom_type.unique()
        if all(gt in ['Point', 'MultiPoint'] for gt in geom_types):
            return 'point'
        return 'polygon'
    elif isinstance(geometry, Point):
        return 'point'
    else:
        return 'polygon'


def merge_geometries(
    gdf: gpd.GeoDataFrame
) -> Union[Polygon, MultiPolygon]:
    """
    Merge all geometries in a GeoDataFrame into a single geometry.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame with geometries to merge.

    Returns
    -------
    Union[Polygon, MultiPolygon]
        Merged geometry.

    Examples
    --------
    >>> merged = merge_geometries(county_boundaries)
    >>> print(f"Merged area: {merged.area}")
    """
    return unary_union(gdf.geometry)


def clip_to_geometry(
    gdf: gpd.GeoDataFrame,
    clip_geometry: Union[Polygon, MultiPolygon, gpd.GeoDataFrame]
) -> gpd.GeoDataFrame:
    """
    Clip a GeoDataFrame to a specified geometry.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        GeoDataFrame to clip.
    clip_geometry : Union[Polygon, MultiPolygon, gpd.GeoDataFrame]
        Geometry to clip to.

    Returns
    -------
    gpd.GeoDataFrame
        Clipped GeoDataFrame.
    """
    if isinstance(clip_geometry, gpd.GeoDataFrame):
        clip_geometry = merge_geometries(clip_geometry)
    
    return gpd.clip(gdf, clip_geometry)
