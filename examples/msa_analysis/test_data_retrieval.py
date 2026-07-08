#!/usr/bin/env python
"""Test script to debug data retrieval issues."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import dataretrieval.nwis as nwis
from pyGWRetrieval import GroundwaterRetrieval, setup_logging
import logging

# Set up logging to see all messages
setup_logging(level=logging.DEBUG)

print("=" * 70)
print("Testing Direct USGS API Calls")
print("=" * 70)

# Test with bounding box around Chicago
min_lon = -87.6181 - 1.5
max_lon = -87.6181 + 1.5
min_lat = 41.8858 - 1.5
max_lat = 41.8858 + 1.5

# Get sites with GW data
sites, _ = nwis.what_sites(bBox=f'{min_lon},{min_lat},{max_lon},{max_lat}', siteType='GW', hasDataTypeCd='gw')
print(f'Found {len(sites)} sites with GW data')

# Get the first 10 site numbers
sites = sites.reset_index()
site_numbers = sites['site_no'].head(10).tolist()
print(f'Testing sites: {site_numbers}')

# Try with a much longer date range
print()
print('Testing gwlevels (2020-present):')
try:
    data, _ = nwis.get_gwlevels(sites=site_numbers, start='2020-01-01', end='2026-01-01')
    print(f'  Retrieved {len(data)} records')
    if len(data) > 0:
        print(data.head())
except Exception as e:
    print(f'  Error: {e}')

print()
print("=" * 70)
print("Testing pyGWRetrieval Module")
print("=" * 70)

# Now test the module
print()
print("Testing get_data_by_zipcode with zip 60601 (Chicago):")
gw = GroundwaterRetrieval(start_date='2020-01-01', data_sources=['gwlevels', 'dv'])
try:
    data = gw.get_data_by_zipcode('60601', buffer_miles=100)
    print(f"  Retrieved {len(data)} records")
    if len(data) > 0:
        print(f"  Unique wells: {data['site_no'].nunique()}")
        if 'data_source' in data.columns:
            print(f"  By source: {data.groupby('data_source').size().to_dict()}")
        print(data.head())
    else:
        print("  No data returned!")
        print(f"  Wells found: {len(gw.wells) if gw.wells is not None else 0}")
except Exception as e:
    import traceback
    print(f"  Error: {e}")
    traceback.print_exc()

