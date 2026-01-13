#!/usr/bin/env python
"""
Example: Advanced Spatial Queries with pyGWRetrieval

This script demonstrates advanced spatial query capabilities including:
- Using shapefiles for queries
- Using GeoJSON files
- Buffer operations
- Spatial filtering

USGS Data Columns Retrieved:
----------------------------
- site_no: USGS site identification number
- lev_dt: Date of water level measurement (YYYY-MM-DD)
- lev_va: Water level in FEET BELOW LAND SURFACE
          (lower = shallower water table, higher = deeper)
- station_nm: Station/well name
- dec_lat_va: Latitude in decimal degrees
- dec_long_va: Longitude in decimal degrees

Coordinate Reference Systems:
-----------------------------
- Data is retrieved in EPSG:4326 (WGS84 geographic coordinates)
- Buffers are calculated using appropriate projected CRS
"""

from pyGWRetrieval import (
    GroundwaterRetrieval,
    TemporalAggregator,
    GroundwaterPlotter,
    get_geometry_from_geojson,
    get_geometry_from_shapefile,
    buffer_geometry,
    setup_logging,
)
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import logging
import json


def example_geojson_polygon():
    """Example: Create and use a GeoJSON polygon for queries."""
    
    print("\n--- Example: GeoJSON Polygon Query ---")
    
    # Create a simple polygon (Diamond Valley, NV area)
    polygon_geojson = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {"name": "Study Area"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-116.5, 39.5],
                    [-116.0, 39.5],
                    [-116.0, 40.0],
                    [-116.5, 40.0],
                    [-116.5, 39.5]
                ]]
            }
        }]
    }
    
    # Save to file
    with open('output/study_area.geojson', 'w') as f:
        json.dump(polygon_geojson, f)
    print("Created: output/study_area.geojson")
    
    # Query using the GeoJSON
    gw = GroundwaterRetrieval(start_date='2020-01-01')
    
    try:
        data = gw.get_data_by_geojson('output/study_area.geojson')
        
        if not data.empty:
            print(f"Retrieved {len(data)} records from {data['site_no'].nunique()} wells")
            print(f"Wells: {data['site_no'].unique()[:5]}")  # First 5 wells
        else:
            print("No data found in the specified area")
            
    except Exception as e:
        print(f"Query failed: {e}")
        print("This may occur if there are no wells in the specified area")


def example_point_buffer():
    """Example: Create buffer around a point."""
    
    print("\n--- Example: Point with Buffer ---")
    
    # Create a point GeoJSON
    point_geojson = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "properties": {"name": "Well Location"},
            "geometry": {
                "type": "Point",
                "coordinates": [-119.5, 39.5]  # Lon, Lat
            }
        }]
    }
    
    # Save to file
    with open('output/point_location.geojson', 'w') as f:
        json.dump(point_geojson, f)
    print("Created: output/point_location.geojson")
    
    # Query with buffer
    gw = GroundwaterRetrieval(start_date='2020-01-01')
    
    try:
        # Point queries require a buffer
        data = gw.get_data_by_geojson(
            'output/point_location.geojson',
            buffer_miles=20  # 20-mile radius
        )
        
        if not data.empty:
            print(f"Retrieved {len(data)} records from {data['site_no'].nunique()} wells")
        else:
            print("No data found within buffer")
            
    except Exception as e:
        print(f"Query failed: {e}")


def example_state_query():
    """Example: Query an entire state."""
    
    print("\n--- Example: State Query ---")
    
    # Query a smaller time period to limit data size
    gw = GroundwaterRetrieval(
        start_date='2023-06-01',
        end_date='2023-12-31'
    )
    
    try:
        # Get data for Nevada (use smaller states for quicker results)
        data = gw.get_data_by_state('NV')
        
        if not data.empty:
            print(f"Retrieved {len(data)} records from {data['site_no'].nunique()} wells")
            
            # Show well distribution by county/area
            if 'station_nm' in data.columns:
                print("\nSample station names:")
                print(data['station_nm'].dropna().unique()[:10])
        else:
            print("No data found for the state in the specified period")
            
    except Exception as e:
        print(f"Query failed: {e}")


def example_specific_sites():
    """Example: Query specific USGS sites."""
    
    print("\n--- Example: Specific Sites Query ---")
    
    # These are example site numbers - replace with actual USGS site numbers
    # You can find site numbers at: https://waterdata.usgs.gov/nwis
    example_sites = [
        '390412119455301',  # Example NV site
        '390144119451301',  # Example NV site
    ]
    
    gw = GroundwaterRetrieval(start_date='2020-01-01')
    
    try:
        data = gw.get_data_by_sites(example_sites)
        
        if not data.empty:
            print(f"Retrieved {len(data)} records from {data['site_no'].nunique()} sites")
            print("\nData preview:")
            print(data[['site_no', 'lev_dt', 'lev_va']].head())
        else:
            print("No data found for specified sites")
            
    except Exception as e:
        print(f"Query failed: {e}")
        print("Note: Example site numbers may not exist. Use actual USGS site numbers.")


def example_buffer_analysis():
    """Example: Spatial buffer analysis."""
    
    print("\n--- Example: Buffer Analysis ---")
    
    # Create a point
    point = Point(-119.7, 39.5)  # Reno, NV area
    
    # Create buffers of different sizes
    buffers = [5, 10, 20]  # miles
    
    for buffer_miles in buffers:
        print(f"\nBuffer: {buffer_miles} miles")
        
        buffered = buffer_geometry(point, buffer_miles)
        bbox = buffered.bounds
        print(f"  Bounding box: {bbox}")
        print(f"  Approximate area: {buffered.area:.4f} sq degrees")


def example_multi_area_comparison():
    """Example: Compare data from multiple areas."""
    
    print("\n--- Example: Multi-Area Comparison ---")
    
    # Define multiple areas (zip codes for simplicity)
    areas = {
        'Carson City': '89701',
        'Reno': '89501',
        'Las Vegas': '89101',
    }
    
    results = {}
    
    for name, zipcode in areas.items():
        print(f"\nQuerying {name} ({zipcode})...")
        
        gw = GroundwaterRetrieval(start_date='2022-01-01')
        
        try:
            data = gw.get_data_by_zipcode(zipcode, buffer_miles=10)
            
            if not data.empty:
                results[name] = {
                    'records': len(data),
                    'wells': data['site_no'].nunique(),
                    'mean_depth': data['lev_va'].mean() if 'lev_va' in data.columns else None,
                }
                print(f"  Found {results[name]['wells']} wells with {results[name]['records']} records")
            else:
                print(f"  No data found")
                results[name] = {'records': 0, 'wells': 0, 'mean_depth': None}
                
        except Exception as e:
            print(f"  Query failed: {e}")
            results[name] = {'records': 0, 'wells': 0, 'mean_depth': None}
    
    # Summary
    print("\n=== Comparison Summary ===")
    print(f"{'Area':<15} {'Wells':>8} {'Records':>10} {'Mean Depth':>12}")
    print("-" * 50)
    for name, stats in results.items():
        mean_str = f"{stats['mean_depth']:.2f}" if stats['mean_depth'] else "N/A"
        print(f"{name:<15} {stats['wells']:>8} {stats['records']:>10} {mean_str:>12}")


def main():
    """Run all spatial query examples."""
    
    setup_logging(level=logging.INFO)
    
    print("=" * 60)
    print("pyGWRetrieval - Advanced Spatial Queries Example")
    print("=" * 60)
    
    # Run examples
    example_buffer_analysis()
    example_geojson_polygon()
    example_point_buffer()
    example_state_query()
    example_specific_sites()
    example_multi_area_comparison()
    
    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == '__main__':
    import os
    os.makedirs('output', exist_ok=True)
    
    main()
