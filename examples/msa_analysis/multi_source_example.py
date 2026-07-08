#!/usr/bin/env python
"""
Example: Multi-Source Groundwater Data Retrieval

This script demonstrates how to retrieve groundwater data from multiple
USGS data sources:
- gwlevels: Field groundwater-level measurements (discrete)
- dv: Daily values (daily statistical summaries)
- iv: Instantaneous values (current/historical observations)

Each source provides different types of data:
- gwlevels: Most accurate but infrequent (manual field readings)
- dv: Daily summaries good for trend analysis
- iv: High-frequency data (typically 15-minute intervals)
"""

import sys
from pathlib import Path

# Add parent directory to path for development
sys.path.insert(0, str(Path(__file__).parent.parent))

from pyGWRetrieval import GroundwaterRetrieval, setup_logging
import logging


def main():
    """Demonstrate multi-source data retrieval."""
    
    # Set up logging
    setup_logging(level=logging.INFO)
    
    print("=" * 70)
    print("pyGWRetrieval - Multi-Source Data Retrieval Example")
    print("=" * 70)
    
    # Configuration
    ZIPCODE = "89701"  # Carson City, Nevada
    BUFFER_MILES = 10
    START_DATE = "2020-01-01"
    
    # =========================================================================
    # Example 1: Default behavior (gwlevels only)
    # =========================================================================
    print("\n" + "-" * 70)
    print("Example 1: Field Measurements Only (gwlevels)")
    print("-" * 70)
    
    gw_default = GroundwaterRetrieval(
        start_date=START_DATE,
        data_sources='gwlevels'  # This is the default
    )
    
    data_gwlevels = gw_default.get_data_by_zipcode(ZIPCODE, buffer_miles=BUFFER_MILES)
    print(f"Retrieved {len(data_gwlevels):,} records from {data_gwlevels['site_no'].nunique()} wells")
    
    if not data_gwlevels.empty:
        print("\nSample columns:", list(data_gwlevels.columns[:10]))
        print(f"Date range: {data_gwlevels['datetime'].min()} to {data_gwlevels['datetime'].max()}")
    
    # =========================================================================
    # Example 2: Daily values only
    # =========================================================================
    print("\n" + "-" * 70)
    print("Example 2: Daily Values Only (dv)")
    print("-" * 70)
    
    gw_daily = GroundwaterRetrieval(
        start_date=START_DATE,
        data_sources='dv'
    )
    
    data_dv = gw_daily.get_data_by_zipcode(ZIPCODE, buffer_miles=BUFFER_MILES)
    print(f"Retrieved {len(data_dv):,} records from daily values")
    
    if not data_dv.empty:
        print(f"Date range: {data_dv['datetime'].min()} to {data_dv['datetime'].max()}")
    
    # =========================================================================
    # Example 3: Instantaneous values (current/historical observations)
    # =========================================================================
    print("\n" + "-" * 70)
    print("Example 3: Instantaneous Values (iv)")
    print("-" * 70)
    
    # Note: iv data is typically available for shorter time periods
    gw_instant = GroundwaterRetrieval(
        start_date="2024-01-01",  # Recent data
        data_sources='iv'
    )
    
    data_iv = gw_instant.get_data_by_zipcode(ZIPCODE, buffer_miles=BUFFER_MILES)
    print(f"Retrieved {len(data_iv):,} records from instantaneous values")
    
    if not data_iv.empty:
        print(f"Date range: {data_iv['datetime'].min()} to {data_iv['datetime'].max()}")
    
    # =========================================================================
    # Example 4: Multiple sources combined
    # =========================================================================
    print("\n" + "-" * 70)
    print("Example 4: Multiple Sources (gwlevels + dv)")
    print("-" * 70)
    
    gw_multi = GroundwaterRetrieval(
        start_date=START_DATE,
        data_sources=['gwlevels', 'dv']  # Both field measurements and daily values
    )
    
    data_multi = gw_multi.get_data_by_zipcode(ZIPCODE, buffer_miles=BUFFER_MILES)
    print(f"Retrieved {len(data_multi):,} total records")
    
    if not data_multi.empty and 'data_source' in data_multi.columns:
        print("\nRecords by source:")
        print(data_multi.groupby('data_source').size())
    
    # =========================================================================
    # Example 5: All sources
    # =========================================================================
    print("\n" + "-" * 70)
    print("Example 5: All Available Sources")
    print("-" * 70)
    
    gw_all = GroundwaterRetrieval(
        start_date="2024-01-01",  # Recent period for all sources
        data_sources='all'
    )
    
    data_all = gw_all.get_data_by_zipcode(ZIPCODE, buffer_miles=BUFFER_MILES)
    print(f"Retrieved {len(data_all):,} total records")
    
    if not data_all.empty and 'data_source' in data_all.columns:
        print("\nRecords by source:")
        print(data_all.groupby('data_source').size())
        
        print("\nWells by source:")
        print(data_all.groupby('data_source')['site_no'].nunique())
    
    # =========================================================================
    # Summary
    # =========================================================================
    print("\n" + "=" * 70)
    print("Summary")
    print("=" * 70)
    print(f"""
Data Sources Available:
  - gwlevels: Field measurements (discrete, most accurate, infrequent)
  - dv: Daily values (daily summaries from continuous sensors)
  - iv: Instantaneous values (high-frequency, 15-60 min intervals)

Usage:
  GroundwaterRetrieval(data_sources='gwlevels')     # Default
  GroundwaterRetrieval(data_sources='dv')            # Daily values only
  GroundwaterRetrieval(data_sources='iv')            # Instantaneous only
  GroundwaterRetrieval(data_sources=['gwlevels', 'dv'])  # Multiple
  GroundwaterRetrieval(data_sources='all')           # All sources

CLI:
  pygwretrieval retrieve --zipcode 89701 --data-sources gwlevels dv
  pygwretrieval retrieve --zipcode 89701 --data-sources all
""")


if __name__ == "__main__":
    main()
